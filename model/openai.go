// Package model contains a pure-Go OpenAI-compatible Chat Completions adapter
// and an optional Pi coding-agent SDK adapter for shared auth and providers.
package model

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
)

const defaultMaxResponseBytes int64 = 8 << 20
const maxSSELineBytes = 1 << 20

// OpenAI calls an OpenAI-compatible Chat Completions endpoint. BaseURL may be
// set to a local server or proxy; it must be an origin or include an API path.
type OpenAI struct {
	BaseURL          string
	APIKey           string
	HTTPClient       *http.Client
	MaxResponseBytes int64
	// Window is the model's context window in tokens, for threshold
	// compaction; zero (unknown) disables it.
	Window int
	// NoImages marks a model that takes no images: they are sent as
	// pi-ai's placeholders instead.
	NoImages bool
	// StrictMode marks an endpoint with strict tool schemas (pi-ai
	// compat.supportsStrictMode, off unless a model says so): tools are sent
	// with a strict field, and constrained ones in the strict subset.
	// Without it no strict field is sent, and a tool that requires
	// constrained sampling fails the request.
	StrictMode bool
	// MaxTokens is the model's maximum output (pi-ai model.maxTokens): sent
	// as max_completion_tokens (or max_tokens where pi-ai sends that),
	// clamped to fit Window with pi-ai's safety margins; zero sends none.
	MaxTokens int
	// Reasoning marks a reasoning model (pi-ai model.reasoning): the
	// request's thinking level, clamped to the levels it supports, is sent
	// in the endpoint's thinking format.
	Reasoning bool
	// Compat is pi-ai's model.compat: settings over what pi-ai detects from
	// the provider and BaseURL, such as the thinking format.
	Compat OpenAICompat
	// SamplingParams and SamplingParamsByThinkingLevel are pi-ai's
	// model.samplingParams and samplingParamsByThinkingLevel: request fields
	// such as temperature or top_p, the level's over the model's, sent last
	// so they override the named ones.
	SamplingParams                map[string]any
	SamplingParamsByThinkingLevel map[string]map[string]any
	// ThinkingLevelMap is pi-ai's model.thinkingLevelMap: the value sent
	// for a level, nil for a level the model does not support; a level it
	// does not name is sent as it is, except xhigh and max, which a model
	// supports only when they are named.
	ThinkingLevelMap map[string]*string
}

var (
	_ durable.ContextWindowModel = OpenAI{}
	_ durable.ImageInputModel    = OpenAI{}
)

// AcceptsImages reports whether images reach the model.
func (o OpenAI) AcceptsImages(durable.ModelRef) bool { return !o.NoImages }

// pi-ai's placeholders for images a model cannot take.
const (
	userImagePlaceholder = "(image omitted: model does not support images)"
	toolImagePlaceholder = "(tool image omitted: model does not support images)"
)

func imageURL(image durable.ImageContent) openAIContentPart {
	return openAIContentPart{Type: "image_url", ImageURL: &openAIImageURL{URL: "data:" + image.MimeType + ";base64," + image.Data}}
}

// ContextWindow reports Window for every model.
func (o OpenAI) ContextWindow(durable.ModelRef) int { return o.Window }

func (o OpenAI) Generate(ctx context.Context, req durable.ModelRequest, onDelta func(string) error) (durable.ModelResponse, error) {
	return o.GenerateEvents(ctx, req, func(event durable.StreamEvent) error {
		if event.Type != "text_delta" || onDelta == nil {
			return nil
		}
		return onDelta(event.Delta)
	})
}

// GenerateEvents streams text, reasoning (as thinking), and tool-call
// events, as pi-ai's openai-completions does (durable.EventModel).
func (o OpenAI) GenerateEvents(ctx context.Context, req durable.ModelRequest, onEvent func(durable.StreamEvent) error) (durable.ModelResponse, error) {
	endpoint, err := completionsURL(o.BaseURL)
	if err != nil {
		return durable.ModelResponse{}, err
	}
	c := o.compat(req.Model)
	retention := cacheRetention(req)
	payload := openAIRequest{Model: req.Model.ID, Messages: make([]openAIMessage, 0, len(req.Messages)), Stream: true, Extra: map[string]any{}}
	if c.supportsUsageInStreaming {
		payload.StreamOptions = &streamOptions{IncludeUsage: true}
	}
	o.compatFields(&payload, req, c, retention)
	// Some providers take no user message right after tool results: an
	// assistant message bridges them (pi-ai requiresAssistantAfterToolResult).
	lastRole := ""
	bridge := func() {
		payload.Messages = append(payload.Messages, openAIMessage{Role: "assistant", Content: "I have processed the tool results."})
	}
	// Images a run of tool results returned, sent after it in a user
	// message: Chat Completions tool messages take only text (pi-ai).
	var toolImages []openAIContentPart
	flushToolImages := func() {
		if len(toolImages) > 0 {
			if c.requiresAssistantAfterToolResult {
				bridge()
			}
			parts := append([]openAIContentPart{{Type: "text", Text: "Attached image(s) from tool result:"}}, toolImages...)
			payload.Messages = append(payload.Messages, openAIMessage{Role: "user", Content: parts})
			toolImages = nil
			lastRole = "user"
		}
	}
	// Without mid-conversation system messages, fold them into one leading
	// message with the replayed prompt and tools, as pi-ai does.
	source := req.Messages
	if !c.supportsMidConvoSystemMessages {
		source = durable.CollapseSystemMessages(req.Messages)
	}
	messages := transformMessages(source, req.Model)
	tools, folded := req.Tools, -1
	if durable.HasToolChanges(req.Messages) {
		tools = durable.CurrentTools(req.Messages)
	}
	if c.supportsMidConvoSystemMessages && c.supportsMidConvoToolAdditions {
		tools, folded = transcriptToolAdditions(req.Messages)
	}
	systemIndex := 0
	// Grammar tools by name: the property their input fills.
	grammars, err := grammarProperties(declaredTools(req.Messages, req.Tools), c.supportsOpenAIGrammarTools)
	if err != nil {
		return durable.ModelResponse{}, err
	}
	// pi-ai sends a reasoning model's instructions as a developer message
	// where the endpoint takes that role.
	instructionRole := "system"
	if o.Reasoning && c.supportsDeveloperRole {
		instructionRole = "developer"
	}
	for i, m := range messages {
		if m.Role == "system" {
			// Tools added after the folded ones are declared in place.
			if folded >= 0 && systemIndex >= folded && len(m.ToolsAdded) > 0 {
				added, err := o.convertTools(m.ToolsAdded, c)
				if err != nil {
					return durable.ModelResponse{}, err
				}
				flushToolImages()
				payload.Messages = append(payload.Messages, openAIMessage{Role: "system", toolsOnly: true, tools: added})
			}
			systemIndex++
			if i == 0 {
				m.Content = durable.SystemText(m)
			} else {
				m.Content = renderSystemMessageUpdate(m)
			}
			if m.Content == "" {
				lastRole = "system"
				continue
			}
			m.Role = instructionRole
		}
		role := m.Role
		switch role {
		case "user", "assistant", "system", "developer", "tool":
		default:
			return durable.ModelResponse{}, sanitizedError(fmt.Sprintf("unsupported message role %q", role), false)
		}
		if role != "tool" {
			flushToolImages()
			if c.requiresAssistantAfterToolResult && lastRole == "toolResult" && role == "user" {
				bridge()
			}
		}
		msg := openAIMessage{Role: role}
		switch {
		case role == "user" && len(m.Images) > 0:
			// Text and images in order; without image input, each run of
			// images becomes one placeholder in place, as pi-ai does.
			var parts []openAIContentPart
			placeholder := false
			for _, part := range durable.OrderedContent(m.Content, m.Images, m.ImageOffsets) {
				switch {
				case part.Image == nil:
					parts = append(parts, openAIContentPart{Type: "text", Text: part.Text})
					placeholder = false
				case o.NoImages:
					if !placeholder {
						parts = append(parts, openAIContentPart{Type: "text", Text: userImagePlaceholder})
					}
					placeholder = true
				default:
					parts = append(parts, imageURL(*part.Image))
				}
			}
			msg.Content = parts
		case role == "tool":
			msg.ToolCallID = m.ToolCallID
			text := m.Content
			if len(m.Images) > 0 && o.NoImages {
				// Each run of images becomes one placeholder where it was.
				var blocks []string
				placeholder := false
				for _, part := range durable.OrderedContent(m.Content, m.Images, m.ImageOffsets) {
					if part.Image == nil {
						blocks = append(blocks, part.Text)
						placeholder = false
					} else if !placeholder {
						blocks = append(blocks, toolImagePlaceholder)
						placeholder = true
					}
				}
				text = strings.Join(blocks, "\n")
			}
			switch {
			case strings.TrimSpace(text) != "":
			case len(m.Images) > 0:
				text = "(see attached image)"
			default:
				text = "(no tool output)"
			}
			msg.Content = text
			// Some providers require the tool's name (pi-ai).
			if c.requiresToolResultName && m.ToolName != "" {
				msg.Name = m.ToolName
			}
			if !o.NoImages {
				for _, image := range m.Images {
					toolImages = append(toolImages, imageURL(image))
				}
			}
		case role == "assistant":
			keep, err := o.assistantMessage(&msg, m, req.Model, c, grammars)
			if err != nil {
				return durable.ModelResponse{}, err
			}
			if !keep {
				continue
			}
		default:
			msg.Content = m.Content
		}
		payload.Messages = append(payload.Messages, msg)
		lastRole = role
		if role == "tool" {
			lastRole = "toolResult"
		}
	}
	flushToolImages()
	offered, err := o.convertTools(tools, c)
	if err != nil {
		return durable.ModelResponse{}, err
	}
	if len(offered) > 0 {
		payload.Tools = offered
		if c.zaiToolStream {
			payload.Extra["tool_stream"] = true
		}
	} else if c.requiresToolsWithToolHistory && hasToolHistory(req.Messages) {
		payload.Tools = []openAITool{}
	}
	if marker := c.cacheControl(retention); marker != nil {
		applyCacheControl(payload.Messages, offered, marker)
	}
	body, err := marshalNoEscape(payload)
	if err != nil {
		return durable.ModelResponse{}, sanitizedError("cannot encode model request", false)
	}
	// Hooks see the body as built, in pi-ai's API name for this format.
	if req.OnPayload != nil {
		replaced, err := req.OnPayload(ctx, "openai-completions", body)
		if err != nil {
			return durable.ModelResponse{}, err
		}
		if replaced != nil {
			body = replaced
		}
	}
	if req.Stream != nil && req.Stream.TimeoutMs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.Stream.TimeoutMs)*time.Millisecond)
		defer cancel()
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return durable.ModelResponse{}, sanitizedError("cannot create model request", false)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	for name, value := range o.requestHeaders(req, c) {
		httpReq.Header.Set(name, value)
	}
	if o.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+o.APIKey)
	}
	client := o.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		if ctx.Err() != nil {
			return durable.ModelResponse{}, ctx.Err()
		}
		// Transport failures are pi-ai's "Connection error." family: transient.
		return durable.ModelResponse{}, sanitizedError("model request failed", true)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Classify what pi-ai would report, then discard the provider's text.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		if ctx.Err() != nil {
			return durable.ModelResponse{}, ctx.Err()
		}
		return durable.ModelResponse{}, classifiedError(fmt.Sprintf("model endpoint returned HTTP %d", resp.StatusCode),
			openAIErrorText(resp.StatusCode, body), req.Model.Provider)
	}
	maxBytes := o.MaxResponseBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxResponseBytes
	}
	limited := &limitedReader{r: resp.Body, remaining: maxBytes}
	response, err := parseSSE(ctx, limited, onEvent, req.Model, c.supportsFinishReason, grammars)
	if err != nil {
		return durable.ModelResponse{}, err
	}
	return response, nil
}

type openAIRequest struct {
	Model                string          `json:"model"`
	Messages             []openAIMessage `json:"messages"`
	Stream               bool            `json:"stream"`
	PromptCacheKey       string          `json:"prompt_cache_key,omitempty"`
	PromptCacheRetention string          `json:"prompt_cache_retention,omitempty"`
	StreamOptions        *streamOptions  `json:"stream_options,omitempty"`
	Store                *bool           `json:"store,omitempty"`
	ReasoningEffort      string          `json:"reasoning_effort,omitempty"`
	// Thinking formats other than reasoning_effort (pi-ai thinkingFormat).
	Thinking            any   `json:"thinking,omitempty"`
	EnableThinking      *bool `json:"enable_thinking,omitempty"`
	ChatTemplateKwargs  any   `json:"chat_template_kwargs,omitempty"`
	Reasoning           any   `json:"reasoning,omitempty"`
	MaxCompletionTokens int   `json:"max_completion_tokens,omitempty"`
	MaxTokens           int   `json:"max_tokens,omitempty"`
	// Tools is []openAITool when set: an empty one is sent as [].
	Tools any `json:"tools,omitempty"`
	// Extra are fields with other names (budget fields, routing, sampling
	// parameters), written last over the named ones.
	Extra map[string]any `json:"-"`
}

// MarshalJSON writes the named fields, then Extra over them.
func (r openAIRequest) MarshalJSON() ([]byte, error) {
	type plain openAIRequest
	named, err := marshalNoEscape(plain(r))
	if err != nil || len(r.Extra) == 0 {
		return named, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(named, &fields); err != nil {
		return nil, err
	}
	for key, value := range r.Extra {
		raw, err := marshalNoEscape(value)
		if err != nil {
			return nil, err
		}
		fields[key] = raw
	}
	return marshalNoEscape(fields)
}

// marshalNoEscape is json.Marshal that writes <, >, and & as they are, as
// JSON.stringify does.
func marshalNoEscape(value any) ([]byte, error) {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(out.Bytes(), []byte("\n")), nil
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}
type openAIMessage struct {
	// toolsOnly marks a tool declaration in place (pi-ai's Kimi-style
	// system message): role and tools, without content.
	toolsOnly bool
	tools     []openAITool
	Role      string `json:"role"`
	// Content is text, content parts, or null for an answer without text.
	Content    any              `json:"content"`
	Name       string           `json:"name,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	ToolCalls  []openAIToolCall `json:"tool_calls,omitempty"`
	// An assistant message's thinking, replayed in the field it came in
	// (pi-ai keeps it for the same model only).
	ReasoningContent *string          `json:"reasoning_content,omitempty"`
	Reasoning        *string          `json:"reasoning,omitempty"`
	ReasoningText    *string          `json:"reasoning_text,omitempty"`
	ReasoningDetails []map[string]any `json:"reasoning_details,omitempty"`
}
type openAIContentPart struct {
	Type         string          `json:"type"`
	Text         string          `json:"text,omitempty"`
	ImageURL     *openAIImageURL `json:"image_url,omitempty"`
	CacheControl map[string]any  `json:"cache_control,omitempty"`
}
type openAIImageURL struct {
	URL string `json:"url"`
}
type openAITool struct {
	Type         string                    `json:"type"`
	Function     *openAIFunctionDefinition `json:"function,omitempty"`
	Custom       *openAICustomTool         `json:"custom,omitempty"`
	CacheControl map[string]any            `json:"cache_control,omitempty"`
}

// openAICustomTool is a grammar-constrained tool (OpenAI custom tools).
type openAICustomTool struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Format      struct {
		Type    string `json:"type"`
		Grammar struct {
			Syntax     string `json:"syntax"`
			Definition string `json:"definition"`
		} `json:"grammar"`
	} `json:"format"`
}
type openAIFunctionDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
	Strict      *bool           `json:"strict,omitempty"`
}

func compactParameters(raw json.RawMessage) []byte {
	var out bytes.Buffer
	if json.Compact(&out, raw) != nil {
		return raw
	}
	return out.Bytes()
}

type openAIToolCall struct {
	ID       string            `json:"id,omitempty"`
	Type     string            `json:"type,omitempty"`
	Function *openAIFunction   `json:"function,omitempty"`
	Custom   *openAICustomCall `json:"custom,omitempty"`
}

// openAICustomCall is a grammar tool's call: its input text.
type openAICustomCall struct {
	Name  string `json:"name"`
	Input string `json:"input"`
}

// streamToolCallDelta is a streamed piece of a tool call: a function's
// arguments text or a custom tool's input text.
type streamToolCallDelta struct {
	Index    *int            `json:"index"`
	ID       string          `json:"id"`
	Function *openAIFunction `json:"function"`
	Custom   *struct {
		Name  string `json:"name"`
		Input string `json:"input"`
	} `json:"custom"`
}
type openAIFunction struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}
type streamChunk struct {
	Error   json.RawMessage `json:"error"`
	Choices []struct {
		Delta struct {
			Content   string                `json:"content"`
			ToolCalls []streamToolCallDelta `json:"tool_calls"`
			// Reasoning fields some endpoints send; pi-ai reads the first
			// non-empty one.
			ReasoningContent string `json:"reasoning_content"`
			Reasoning        string `json:"reasoning"`
			ReasoningText    string `json:"reasoning_text"`
			// OpenRouter's structured reasoning, replay data only.
			ReasoningDetails []json.RawMessage `json:"reasoning_details"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
		// Cache reads, wherever a provider puts them (pi-ai
		// parseChunkUsage): OpenAI and OpenRouter in the details, DeepSeek
		// as prompt_cache_hit_tokens, Kimi at the top.
		CachedTokens         *int64 `json:"cached_tokens"`
		PromptCacheHitTokens *int64 `json:"prompt_cache_hit_tokens"`
		PromptTokensDetails  *struct {
			CachedTokens     *int64 `json:"cached_tokens"`
			CacheWriteTokens int64  `json:"cache_write_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
}

var errResponseTooLarge error = &providerError{message: "model response exceeds configured size limit"}

type limitedReader struct {
	r         io.Reader
	remaining int64
}

func (r *limitedReader) Read(p []byte) (int, error) {
	if r.remaining < 0 {
		return 0, errResponseTooLarge
	}
	if int64(len(p)) > r.remaining+1 {
		p = p[:r.remaining+1]
	}
	n, err := r.r.Read(p)
	r.remaining -= int64(n)
	if r.remaining < 0 {
		return n, errResponseTooLarge
	}
	return n, err
}

type streamTool struct {
	id, name  string
	index     int
	arguments strings.Builder
	// A grammar tool's call: its input text, which fills property.
	custom      bool
	property    string
	input       strings.Builder
	jsonStarted bool
}

// customPiece is pi-ai's appendGrammarToolInputJsonDelta: the JSON text of
// the arguments {property: input} that a piece of input adds.
func (t *streamTool) customPiece(input string) string {
	piece := ""
	if !t.jsonStarted {
		t.jsonStarted = true
		piece = "{" + jsQuote(t.property) + `:"`
	}
	t.input.WriteString(input)
	quoted := jsQuote(input)
	return piece + quoted[1:len(quoted)-1]
}

// parseSSE reads a Chat Completions stream as pi-ai does. The answer ends
// with the stream: its finish reason says how, and without one it is an
// error, or with supportsFinishReason false a stop or a tool use.
func parseSSE(ctx context.Context, r io.Reader, onEvent func(durable.StreamEvent) error, model durable.ModelRef, supportsFinishReason bool, grammars map[string]string) (durable.ModelResponse, error) {
	provider := model.Provider
	response := durable.ModelResponse{}
	// The field the thinking came in, and OpenRouter's reasoning details,
	// for replaying them to the same model.
	reasoningField := ""
	var reasoningDetails []map[string]any
	// Tool calls in content order, found by stream index or else by ID.
	var tools []*streamTool
	byIndex, byID := map[int]*streamTool{}, map[string]*streamTool{}
	emit := func(event durable.StreamEvent) error {
		if onEvent == nil {
			return nil
		}
		return onEvent(event)
	}
	// Content indices as pi-ai assigns them: blocks in order of appearance.
	blocks := 0
	thinkingIndex, textIndex := -1, -1
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), maxSSELineBytes)
	var data []string
	gotDone := false
	hasFinishReason := false
	finishError := ""
	process := func() error {
		if len(data) == 0 {
			return nil
		}
		text := strings.Join(data, "\n")
		data = nil
		if text == "[DONE]" {
			gotDone = true
			return nil
		}
		var chunk streamChunk
		if err := json.Unmarshal([]byte(text), &chunk); err != nil {
			return sanitizedError("invalid event in model stream", false)
		}
		var streamError any
		if len(chunk.Error) > 0 && json.Unmarshal(chunk.Error, &streamError) == nil && truthyJSON(streamError) {
			return classifiedError("model stream reported an error", streamErrorText(chunk.Error), provider)
		}
		if usage := chunk.Usage; usage != nil {
			var read, write int64
			switch {
			case usage.PromptTokensDetails != nil && usage.PromptTokensDetails.CachedTokens != nil:
				read = *usage.PromptTokensDetails.CachedTokens
			case usage.PromptCacheHitTokens != nil:
				read = *usage.PromptCacheHitTokens
			case usage.CachedTokens != nil:
				read = *usage.CachedTokens
			}
			if usage.PromptTokensDetails != nil {
				write = usage.PromptTokensDetails.CacheWriteTokens
			}
			response.Usage.InputTokens = max(0, usage.PromptTokens-read-write)
			response.Usage.OutputTokens = usage.CompletionTokens
			response.Usage.CacheReadTokens, response.Usage.CacheWriteTokens = read, write
		}
		for _, choice := range chunk.Choices {
			if choice.FinishReason != nil && *choice.FinishReason != "" {
				// pi-ai mapStopReason: other reasons end the answer as an
				// error.
				hasFinishReason = true
				finishError = ""
				switch *choice.FinishReason {
				case "length":
					response.StopReason = "length"
				case "tool_calls", "function_call":
					response.StopReason = "toolUse"
				case "stop", "end":
					response.StopReason = "stop"
				default:
					finishError = "Provider finish_reason: " + *choice.FinishReason
				}
			}
			// Reasoning before content: a proxy can send both in one delta,
			// and the thinking comes first.
			reasoning, field := choice.Delta.ReasoningContent, "reasoning_content"
			if reasoning == "" {
				reasoning, field = choice.Delta.Reasoning, "reasoning"
			}
			if reasoning == "" {
				reasoning, field = choice.Delta.ReasoningText, "reasoning_text"
			}
			if reasoning != "" {
				if reasoningField == "" {
					reasoningField = field
				}
				if thinkingIndex < 0 {
					thinkingIndex, blocks = blocks, blocks+1
				}
				response.Thinking += reasoning
				if err := emit(durable.StreamEvent{Type: "thinking_delta", Index: thinkingIndex, Delta: reasoning}); err != nil {
					return err
				}
			}
			for _, raw := range choice.Delta.ReasoningDetails {
				var detail map[string]any
				if json.Unmarshal(raw, &detail) != nil || !validReasoningDetail(detail) {
					continue
				}
				// A thinking block holds them, even without thinking text.
				if thinkingIndex < 0 {
					thinkingIndex, blocks = blocks, blocks+1
				}
				reasoningDetails = appendReasoningDetail(reasoningDetails, detail)
			}
			if choice.Delta.Content != "" {
				if textIndex < 0 {
					textIndex, blocks = blocks, blocks+1
				}
				response.Content += choice.Delta.Content
				if err := emit(durable.StreamEvent{Type: "text_delta", Index: textIndex, Delta: choice.Delta.Content}); err != nil {
					return err
				}
			}
			for _, delta := range choice.Delta.ToolCalls {
				name := ""
				if delta.Function != nil {
					name = delta.Function.Name
				} else if delta.Custom != nil {
					name = delta.Custom.Name
				}
				custom := delta.Custom != nil && delta.Function == nil
				var t *streamTool
				if delta.Index != nil {
					t = byIndex[*delta.Index]
				}
				if t == nil && delta.ID != "" {
					t = byID[delta.ID]
				}
				if t == nil {
					t = &streamTool{index: blocks, id: delta.ID, name: name}
					blocks++
					tools = append(tools, t)
					if err := emit(durable.StreamEvent{Type: "toolcall_start", Index: t.index, ID: t.id, Name: t.name}); err != nil {
						return err
					}
				}
				if delta.Index != nil && byIndex[*delta.Index] == nil {
					byIndex[*delta.Index] = t
				}
				if delta.ID != "" {
					byID[delta.ID] = t
					if t.id == "" {
						t.id = delta.ID
					}
				}
				if t.name == "" && name != "" {
					t.name = name
				}
				if custom && !t.custom {
					t.custom, t.property = true, "input"
					if property, ok := grammars[t.name]; ok {
						t.property = property
					}
				}
				piece := ""
				switch {
				case delta.Function != nil && delta.Function.Arguments != "" && !t.custom:
					piece = delta.Function.Arguments
				case custom && delta.Custom.Input != "" && t.custom:
					piece = t.customPiece(delta.Custom.Input)
				}
				t.arguments.WriteString(piece)
				if piece != "" {
					if err := emit(durable.StreamEvent{Type: "toolcall_delta", Index: t.index, Delta: piece}); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return durable.ModelResponse{}, err
		}
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			if err := process(); err != nil {
				return durable.ModelResponse{}, err
			}
			if gotDone {
				break
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			v := strings.TrimPrefix(line, "data:")
			if strings.HasPrefix(v, " ") {
				v = v[1:]
			}
			data = append(data, v)
		}
	}
	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			return durable.ModelResponse{}, ctx.Err()
		}
		if errors.Is(err, errResponseTooLarge) {
			return durable.ModelResponse{}, errResponseTooLarge
		}
		if errors.Is(err, bufio.ErrTooLong) {
			return durable.ModelResponse{}, sanitizedError("model stream line exceeded the size limit", false)
		}
		// A connection that drops mid-stream is transient, as pi-ai's
		// "terminated" and "other side closed" are.
		return durable.ModelResponse{}, sanitizedError("model stream read failed", true)
	}
	if !gotDone {
		if err := process(); err != nil {
			return durable.ModelResponse{}, err
		}
	}
	for _, t := range tools {
		// A grammar call's arguments close with its input.
		if t.custom {
			piece := `"}`
			if !t.jsonStarted {
				t.jsonStarted = true
				piece = "{" + jsQuote(t.property) + `:""}`
			}
			if err := emit(durable.StreamEvent{Type: "toolcall_delta", Index: t.index, Delta: piece}); err != nil {
				return durable.ModelResponse{}, err
			}
		}
		if err := emit(durable.StreamEvent{Type: "toolcall_end", Index: t.index}); err != nil {
			return durable.ModelResponse{}, err
		}
	}
	for _, t := range tools {
		if t.custom {
			args, _ := marshalNoEscape(map[string]string{t.property: t.input.String()})
			response.ToolCalls = append(response.ToolCalls, durable.ToolCall{ID: t.id, Name: t.name, Arguments: args})
			continue
		}
		// Malformed arguments are the model's error, reported back to it by
		// tool validation (pi-ai parseToolCallArguments).
		args, argumentsError := durable.ParseToolCallArguments(t.arguments.String())
		response.ToolCalls = append(response.ToolCalls, durable.ToolCall{ID: t.id, Name: t.name, Arguments: args, ArgumentsError: argumentsError})
	}
	// Which model answered, for replaying its thinking and keeping its tool
	// call IDs (pi-ai compares an answer's provider and model).
	if response.Thinking != "" || len(response.ToolCalls) > 0 || len(reasoningDetails) > 0 {
		response.ProviderData, _ = json.Marshal(openAIReplay{Format: openAIReplayFormat, Provider: model.Provider, Model: model.ID,
			ReasoningField: reasoningField, ReasoningDetails: reasoningDetails})
	}
	if !hasFinishReason && !supportsFinishReason {
		response.StopReason = "stop"
		if len(response.ToolCalls) > 0 {
			response.StopReason = "toolUse"
		}
	}
	if finishError != "" {
		return response, classifiedError(finishError, finishError, provider)
	}
	if !hasFinishReason && supportsFinishReason {
		// pi-ai's error for a stream that ended early: transient.
		return durable.ModelResponse{}, sanitizedError("Stream ended without finish_reason", true)
	}
	return response, nil
}

func completionsURL(base string) (string, error) {
	if base == "" {
		base = "https://api.openai.com/v1"
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", sanitizedError("invalid model BaseURL", false)
	}
	path := strings.TrimRight(u.Path, "/")
	if !strings.HasSuffix(path, "/chat/completions") {
		path += "/chat/completions"
	}
	u.Path = path
	return u.String(), nil
}

// userAgent is pi-ai's User-Agent for a request identity: "short" is pi,
// {"custom": value} is the value, and "detailed" (the default) names the
// version and the Go runtime. Without an identity the HTTP client's default
// stays. Provider attribution headers are not sent.
func userAgent(identity *durable.ClientIdentity) string {
	if identity == nil {
		return ""
	}
	var mode string
	if json.Unmarshal(identity.UserAgent, &mode) == nil && mode == "short" {
		return "pi"
	}
	var custom struct {
		Custom *string `json:"custom"`
	}
	if json.Unmarshal(identity.UserAgent, &custom) == nil && custom.Custom != nil {
		return *custom.Custom
	}
	name := "pi"
	if identity.Version != "" {
		name += "/" + identity.Version
	}
	return fmt.Sprintf("%s (%s; go/%s; %s)", name, runtime.GOOS, strings.TrimPrefix(runtime.Version(), "go"), runtime.GOARCH)
}

// cacheRetention is pi-ai's resolveCacheRetention: the request's, else
// PI_CACHE_RETENTION=long, else short.
func cacheRetention(req durable.ModelRequest) string {
	if req.Stream != nil && req.Stream.CacheRetention != "" {
		return req.Stream.CacheRetention
	}
	if os.Getenv("PI_CACHE_RETENTION") == "long" {
		return "long"
	}
	return "short"
}

// compatFields sets the request fields pi-ai's openai-completions buildParams
// derives from the compat: store, the prompt cache key and retention, the
// output limit, the thinking level in the compat's format and its token
// budget, routing, vLLM's priority, and the model's sampling parameters.
func (o OpenAI) compatFields(payload *openAIRequest, req durable.ModelRequest, c completionsCompat, retention string) {
	if c.supportsStore {
		off := false
		payload.Store = &off
	}
	cacheKey := false
	switch c.promptCacheKeyMode {
	case "enabled":
		cacheKey = true
	case "auto":
		cacheKey = strings.Contains(o.baseURL(), "api.openai.com") || retention == "long" && c.supportsLongCacheRetention
	}
	if retention != "none" && cacheKey && req.SessionID != "" {
		key := []rune(req.SessionID)
		if len(key) > 64 {
			key = key[:64]
		}
		payload.PromptCacheKey = string(key)
	}
	if retention == "long" && c.supportsLongCacheRetention {
		payload.PromptCacheRetention = "24h"
	}
	limit := o.outputLimit(req.Messages)
	if limit > 0 {
		if c.maxTokensField == "max_tokens" {
			payload.MaxTokens = limit
		} else {
			payload.MaxCompletionTokens = limit
		}
	}
	// The Harness sends no reasoning option for off (upstream generation),
	// so off is never clamped to a level the model supports.
	effort := req.ThinkingLevel
	if effort != "" && effort != "off" {
		effort = o.clampThinkingLevel(effort)
	}
	if effort == "off" {
		effort = ""
	}
	var budgets map[string]int
	if req.Stream != nil {
		budgets = req.Stream.ThinkingBudgets
	}
	budget := o.thinkingBudget(effort, budgets, limit)
	o.thinkingFields(payload, c, effort, budget)
	// A reasoning cap independent of the format: the same server can serve
	// several (pi-ai thinkingTokenBudgetField).
	if c.thinkingTokenBudgetField != "" && budget > 0 {
		payload.Extra[c.thinkingTokenBudgetField] = budget
	}
	if len(c.openRouterRouting) > 0 {
		payload.Extra["provider"] = c.openRouterRouting
	}
	if r := c.gatewayRouting; r != nil && (len(r.Only) > 0 || len(r.Order) > 0) {
		payload.Extra["providerOptions"] = map[string]any{"gateway": r}
	}
	if c.vllmPriority != nil {
		payload.Extra["priority"] = *c.vllmPriority
	}
	// Last, so they override the named fields (pi-ai resolveSamplingParams).
	level := effort
	if level == "" {
		level = "off"
	}
	for key, value := range o.SamplingParams {
		payload.Extra[key] = value
	}
	for key, value := range o.SamplingParamsByThinkingLevel[o.clampThinkingLevel(level)] {
		payload.Extra[key] = value
	}
}

// openAIReplay is the ProviderData model.OpenAI stores on an answer with
// thinking: the model that gave it, and the field the thinking came in.
type openAIReplay struct {
	Format         string `json:"format"`
	Provider       string `json:"provider"`
	Model          string `json:"model"`
	ReasoningField string `json:"reasoningField,omitempty"`
	// ReasoningDetails are OpenRouter's reasoning_details, replayed as they
	// came (pi-ai keeps them in the thinking signature).
	ReasoningDetails []map[string]any `json:"reasoningDetails,omitempty"`
}

const openAIReplayFormat = "openai-completions-v1"

// assistantMessage converts an answer as pi-ai's convertMessages does:
// its text, its thinking for the model that gave it only (pi-ai
// transformMessages drops another model's thinking) in the field it came in
// or as text, its tool calls, and an empty reasoning_content where the
// compat requires one. False for an answer without content or tool calls,
// which is not sent.
func (o OpenAI) assistantMessage(msg *openAIMessage, m durable.Message, model durable.ModelRef, c completionsCompat, grammars map[string]string) (bool, error) {
	if c.requiresAssistantAfterToolResult || c.requiresNonNullAssistantContent {
		msg.Content = ""
	}
	text := ""
	if strings.TrimSpace(m.Content) != "" {
		text = m.Content
	}
	replay, sameModel := sameOpenAIModel(m, model)
	details := sameModel && len(replay.ReasoningDetails) > 0
	if details {
		msg.ReasoningDetails = replay.ReasoningDetails
	}
	if strings.TrimSpace(m.Thinking) != "" && sameModel {
		if c.requiresThinkingAsText {
			parts := []openAIContentPart{{Type: "text", Text: m.Thinking}}
			if text != "" {
				parts = append(parts, openAIContentPart{Type: "text", Text: text})
			}
			msg.Content = parts
		} else {
			if text != "" {
				msg.Content = text
			}
			thinking := m.Thinking
			field := replay.ReasoningField
			if details {
				field = "" // the details are the replay data
			}
			switch field {
			case "reasoning_content":
				msg.ReasoningContent = &thinking
			case "reasoning":
				msg.Reasoning = &thinking
			case "reasoning_text":
				msg.ReasoningText = &thinking
			}
		}
	} else if text != "" {
		msg.Content = text
	}
	for _, call := range m.ToolCalls {
		args := call.Arguments
		if len(args) == 0 {
			args = json.RawMessage(`{}`)
		}
		if property, ok := grammars[call.Name]; ok {
			// A grammar tool's call goes back as its input text.
			var arguments map[string]any
			_ = json.Unmarshal(args, &arguments)
			input, ok := arguments[property].(string)
			if !ok {
				return false, sanitizedError(fmt.Sprintf("Grammar tool call %q requires argument %q to be a string.", call.Name, property), false)
			}
			msg.ToolCalls = append(msg.ToolCalls, openAIToolCall{ID: call.ID, Type: "custom", Custom: &openAICustomCall{Name: call.Name, Input: input}})
			continue
		}
		msg.ToolCalls = append(msg.ToolCalls, openAIToolCall{ID: call.ID, Type: "function", Function: &openAIFunction{Name: call.Name, Arguments: string(args)}})
	}
	if c.requiresReasoningContent && o.Reasoning && msg.ReasoningContent == nil {
		empty := ""
		msg.ReasoningContent = &empty
	}
	switch content := msg.Content.(type) {
	case string:
		return content != "" || len(msg.ToolCalls) > 0, nil
	case []openAIContentPart:
		return len(content) > 0 || len(msg.ToolCalls) > 0, nil
	}
	return len(msg.ToolCalls) > 0, nil
}

func (o OpenAI) baseURL() string {
	if o.BaseURL == "" {
		return "https://api.openai.com/v1"
	}
	return o.BaseURL
}

// thinkingFields sends the thinking level in the compat's format, as
// pi-ai's openai-completions buildParams does for a reasoning model; effort
// is "" for none.
func (o OpenAI) thinkingFields(payload *openAIRequest, c completionsCompat, effort string, budget int) {
	if !o.Reasoning {
		return
	}
	supportsEffort := c.supportsReasoningEffort
	// mapped is thinkingLevelMap[effort] ?? effort.
	mapped := func(level string) string {
		if value, ok := o.ThinkingLevelMap[level]; ok && value != nil {
			return *value
		}
		return level
	}
	// offValue is thinkingLevelMap.off: absent, null, or a string.
	offValue, offNamed := o.ThinkingLevelMap["off"]
	offNull := offNamed && offValue == nil
	switch c.thinkingFormat {
	case "zai":
		if effort != "" {
			payload.Thinking = map[string]any{"type": "enabled", "clear_thinking": false}
			if value, named := o.ThinkingLevelMap[effort]; supportsEffort && !(named && value == nil) {
				payload.ReasoningEffort = mapped(effort)
			}
		} else {
			payload.Thinking = map[string]any{"type": "disabled"}
		}
	case "qwen":
		on := effort != ""
		payload.EnableThinking = &on
		if on && supportsEffort {
			payload.ReasoningEffort = mapped(effort)
		}
	case "qwen-chat-template":
		payload.ChatTemplateKwargs = map[string]any{"enable_thinking": effort != "", "preserve_thinking": true}
	case "chat-template":
		if values := o.chatTemplateValues(c.chatTemplateKwargs, effort, budget); values != nil {
			payload.ChatTemplateKwargs = values
		}
	case "baseten":
		if values := o.chatTemplateValues(c.chatTemplateArgs, effort, budget); values != nil {
			payload.Extra["chat_template_args"] = values
		}
		if supportsEffort {
			level := effort
			if level == "" {
				level = "off"
			}
			value, named := o.ThinkingLevelMap[level]
			switch {
			case !named && effort != "":
				payload.ReasoningEffort = effort
			case named && value != nil:
				payload.ReasoningEffort = *value
			}
		}
	case "deepseek":
		if effort != "" {
			payload.Thinking = map[string]any{"type": "enabled"}
		} else if !offNull {
			payload.Thinking = map[string]any{"type": "disabled"}
		}
		if effort != "" && supportsEffort {
			payload.ReasoningEffort = mapped(effort)
		}
	case "openrouter":
		if effort != "" {
			payload.Reasoning = map[string]any{"effort": mapped(effort)}
		} else if !offNull {
			off := "none"
			if offValue != nil {
				off = *offValue
			}
			payload.Reasoning = map[string]any{"effort": off}
		}
	case "ant-ling":
		if value, ok := o.ThinkingLevelMap[effort]; effort != "" && ok && value != nil {
			payload.Reasoning = map[string]any{"effort": *value}
		}
	case "together":
		payload.Reasoning = map[string]any{"enabled": effort != ""}
		if effort != "" && supportsEffort {
			payload.ReasoningEffort = mapped(effort)
		}
	case "string-thinking":
		if effort != "" {
			payload.Thinking = mapped(effort)
		} else if !offNull {
			off := "none"
			if offValue != nil {
				off = *offValue
			}
			payload.Thinking = off
		}
	default:
		if effort != "" && supportsEffort {
			payload.ReasoningEffort = mapped(effort)
		} else if effort == "" && supportsEffort && offValue != nil {
			payload.ReasoningEffort = *offValue
		}
	}
}

// outputLimit is MaxTokens fit into Window, leaving room for the input being
// counted higher than estimated (pi-ai clampMaxTokensToContext).
func (o OpenAI) outputLimit(messages []durable.Message) int {
	if o.MaxTokens <= 0 {
		return 0
	}
	if o.Window <= 0 {
		return max(1, o.MaxTokens)
	}
	const safetyTokens, safetyRatio, minWithRatio = 4096, 0.15, 32768
	input := durable.EstimateContextTokens(messages)
	margin := max(safetyTokens, int(math.Ceil(float64(input)*safetyRatio)))
	room := o.Window - input - margin
	if room < minWithRatio {
		room = min(minWithRatio, o.Window-input-safetyTokens)
	}
	return min(o.MaxTokens, max(1, room))
}

// thinkingLevels are pi-ai's extended thinking levels, in order.
var thinkingLevels = []string{"off", "minimal", "low", "medium", "high", "xhigh", "max"}

// clampThinkingLevel is pi-ai's clampThinkingLevel: a supported level stays;
// another becomes the nearest supported one above it, else below it.
func (o OpenAI) clampThinkingLevel(level string) string {
	if level == "" {
		level = "off"
	}
	supported := func(candidate string) bool {
		if !o.Reasoning {
			return candidate == "off"
		}
		mapped, named := o.ThinkingLevelMap[candidate]
		if named && mapped == nil {
			return false
		}
		if candidate == "xhigh" || candidate == "max" {
			return named
		}
		return true
	}
	first := "off"
	for _, candidate := range thinkingLevels {
		if supported(candidate) {
			first = candidate
			break
		}
	}
	if supported(level) {
		return level
	}
	index := -1
	for i, candidate := range thinkingLevels {
		if candidate == level {
			index = i
		}
	}
	if index < 0 {
		return first
	}
	for i := index; i < len(thinkingLevels); i++ {
		if supported(thinkingLevels[i]) {
			return thinkingLevels[i]
		}
	}
	for i := index - 1; i >= 0; i-- {
		if supported(thinkingLevels[i]) {
			return thinkingLevels[i]
		}
	}
	return first
}

// convertTools is pi-ai's convertTools: function tools, sent in strict mode
// with their schema in the strict subset where the endpoint takes strict
// tools and the tool asks for constrained sampling.
func (o OpenAI) convertTools(tools []durable.ToolSpec, c completionsCompat) ([]openAITool, error) {
	if len(tools) == 0 {
		return nil, nil
	}
	offered := make([]openAITool, 0, len(tools))
	for _, tool := range tools {
		grammar, err := resolveGrammar(tool, c.supportsOpenAIGrammarTools)
		if err != nil {
			return nil, sanitizedError(err.Error(), false)
		}
		if grammar != nil {
			custom := &openAICustomTool{Name: tool.Name, Description: tool.Description}
			custom.Format.Type = "grammar"
			custom.Format.Grammar.Syntax, custom.Format.Grammar.Definition = grammar.syntax, grammar.definition
			offered = append(offered, openAITool{Type: "custom", Custom: custom})
			continue
		}
		var parameters any
		if len(tool.Parameters) == 0 || !json.Valid(tool.Parameters) {
			return nil, sanitizedError(fmt.Sprintf("invalid parameters schema for tool %q", tool.Name), false)
		}
		if err := json.Unmarshal(tool.Parameters, &parameters); err != nil {
			return nil, sanitizedError(fmt.Sprintf("invalid parameters schema for tool %q", tool.Name), false)
		}
		definition := openAIFunctionDefinition{Name: tool.Name, Description: tool.Description, Parameters: json.RawMessage(compactParameters(tool.Parameters))}
		if o.StrictMode {
			strict, err := strictSampling(tool, true)
			if err != nil {
				return nil, sanitizedError(err.Error(), false)
			}
			on := strict != nil && *strict
			if on {
				converted, err := makeStrictSchema(tool.Parameters)
				if err != nil {
					return nil, sanitizedError(err.Error(), false)
				}
				definition.Parameters = converted
			}
			definition.Strict = &on
		} else if _, err := strictSampling(tool, false); err != nil {
			return nil, sanitizedError(err.Error(), false)
		}
		offered = append(offered, openAITool{Type: "function", Function: &definition})
	}
	return offered, nil
}

// MarshalJSON writes a tool declaration in place without content.
func (m openAIMessage) MarshalJSON() ([]byte, error) {
	if m.toolsOnly {
		return marshalNoEscape(struct {
			Role  string       `json:"role"`
			Tools []openAITool `json:"tools"`
		}{m.Role, m.tools})
	}
	type plain openAIMessage
	return marshalNoEscape(plain(m))
}

// renderSystemMessageUpdate is pi-ai's: a later system message's text, then
// each section's change.
func renderSystemMessageUpdate(m durable.Message) string {
	var parts []string
	if m.Content != "" {
		parts = append(parts, m.Content)
	}
	for _, section := range m.Sections {
		if section.Text == nil {
			parts = append(parts, `Removed system prompt section "`+section.Name+`".`)
		} else {
			parts = append(parts, `Updated system prompt section "`+section.Name+`":`+"\n\n"+*section.Text)
		}
	}
	return strings.Join(parts, "\n\n")
}

// transcriptToolAdditions is pi-ai's resolveTranscriptTools with additions:
// the request offers the tools as of the last removal or redeclaration (and
// at least the leading system message's), and later system messages declare
// their added tools in place. folded counts the system messages the request
// tools hold.
func transcriptToolAdditions(messages []durable.Message) ([]durable.ToolSpec, int) {
	var system []durable.Message
	for _, m := range messages {
		if m.Role == "system" {
			system = append(system, m)
		}
	}
	last := -1
	declared := map[string]bool{}
	for i, m := range system {
		if len(m.ToolsRemoved) > 0 {
			last = i
		}
		for _, tool := range m.ToolsAdded {
			if declared[tool.Name] {
				last = i
			}
			declared[tool.Name] = true
		}
	}
	folded := last + 1
	if len(messages) > 0 && messages[0].Role == "system" {
		folded = max(folded, 1)
	}
	return durable.CurrentTools(system[:folded]), folded
}
