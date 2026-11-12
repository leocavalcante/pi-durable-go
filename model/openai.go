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
	// as reasoning_effort (pi-ai's OpenAI-style format).
	Reasoning bool
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
	payload := openAIRequest{Model: req.Model.ID, Messages: make([]openAIMessage, 0, len(req.Messages)), Stream: true, StreamOptions: &streamOptions{IncludeUsage: true}}
	o.compatFields(&payload, req)
	// Images a run of tool results returned, sent after it in a user
	// message: Chat Completions tool messages take only text (pi-ai).
	var toolImages []openAIContentPart
	flushToolImages := func() {
		if len(toolImages) > 0 {
			parts := append([]openAIContentPart{{Type: "text", Text: "Attached image(s) from tool result:"}}, toolImages...)
			payload.Messages = append(payload.Messages, openAIMessage{Role: "user", Content: parts})
			toolImages = nil
		}
	}
	// Chat Completions takes no system messages mid-conversation: fold them
	// into one leading message with the replayed prompt and tools, as pi-ai
	// does for such providers.
	messages := durable.CollapseSystemMessages(req.Messages)
	tools := req.Tools
	if durable.HasToolChanges(req.Messages) {
		tools = durable.CurrentTools(req.Messages)
	}
	for _, m := range messages {
		if m.Role == "system" {
			m.Content = durable.SystemText(m)
			if m.Content == "" {
				continue
			}
		}
		role := m.Role
		switch role {
		case "user", "assistant", "system", "developer", "tool":
		default:
			return durable.ModelResponse{}, sanitizedError(fmt.Sprintf("unsupported message role %q", role), false)
		}
		if role != "tool" {
			flushToolImages()
		}
		msg := openAIMessage{Role: role}
		if m.Content != "" {
			msg.Content = m.Content
		}
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
			if !o.NoImages {
				for _, image := range m.Images {
					toolImages = append(toolImages, imageURL(image))
				}
			}
		}
		for _, call := range m.ToolCalls {
			args := call.Arguments
			if len(args) == 0 {
				args = json.RawMessage(`{}`)
			}
			msg.ToolCalls = append(msg.ToolCalls, openAIToolCall{ID: call.ID, Type: "function", Function: openAIFunction{Name: call.Name, Arguments: string(args)}})
		}
		payload.Messages = append(payload.Messages, msg)
	}
	flushToolImages()
	if len(tools) > 0 {
		payload.Tools = make([]openAITool, 0, len(tools))
		for _, tool := range tools {
			var parameters any
			if len(tool.Parameters) == 0 || !json.Valid(tool.Parameters) {
				return durable.ModelResponse{}, sanitizedError(fmt.Sprintf("invalid parameters schema for tool %q", tool.Name), false)
			}
			if err := json.Unmarshal(tool.Parameters, &parameters); err != nil {
				return durable.ModelResponse{}, sanitizedError(fmt.Sprintf("invalid parameters schema for tool %q", tool.Name), false)
			}
			definition := openAIFunctionDefinition{Name: tool.Name, Description: tool.Description, Parameters: json.RawMessage(compactParameters(tool.Parameters))}
			// pi-ai sends constrained JSON-schema tools in strict mode, with
			// their schema in the strict subset, and strict false otherwise.
			if o.StrictMode {
				strict, err := strictSampling(tool, true)
				if err != nil {
					return durable.ModelResponse{}, sanitizedError(err.Error(), false)
				}
				on := strict != nil && *strict
				if on {
					converted, err := makeStrictSchema(tool.Parameters)
					if err != nil {
						return durable.ModelResponse{}, sanitizedError(err.Error(), false)
					}
					definition.Parameters = converted
				}
				definition.Strict = &on
			} else if _, err := strictSampling(tool, false); err != nil {
				return durable.ModelResponse{}, sanitizedError(err.Error(), false)
			}
			payload.Tools = append(payload.Tools, openAITool{Type: "function", Function: definition})
		}
	}
	body, err := json.Marshal(payload)
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
	if ua := userAgent(req.Identity); ua != "" {
		httpReq.Header.Set("User-Agent", ua)
	}
	if req.Stream != nil {
		for name, value := range req.Stream.Headers {
			httpReq.Header.Set(name, value)
		}
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
	response, err := parseSSE(ctx, limited, onEvent, req.Model.Provider)
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
	MaxCompletionTokens  int             `json:"max_completion_tokens,omitempty"`
	MaxTokens            int             `json:"max_tokens,omitempty"`
	Tools                []openAITool    `json:"tools,omitempty"`
}
type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}
type openAIMessage struct {
	Role string `json:"role"`
	// Content is text, or content parts for a user message with images.
	Content    any              `json:"content,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	ToolCalls  []openAIToolCall `json:"tool_calls,omitempty"`
}
type openAIContentPart struct {
	Type     string          `json:"type"`
	Text     string          `json:"text,omitempty"`
	ImageURL *openAIImageURL `json:"image_url,omitempty"`
}
type openAIImageURL struct {
	URL string `json:"url"`
}
type openAITool struct {
	Type     string                   `json:"type"`
	Function openAIFunctionDefinition `json:"function"`
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
	Index    int            `json:"index,omitempty"`
	ID       string         `json:"id,omitempty"`
	Type     string         `json:"type,omitempty"`
	Function openAIFunction `json:"function"`
}
type openAIFunction struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}
type streamChunk struct {
	Error   json.RawMessage `json:"error"`
	Choices []struct {
		Delta struct {
			Content   string           `json:"content"`
			ToolCalls []openAIToolCall `json:"tool_calls"`
			// Reasoning fields some endpoints send; pi-ai reads the first
			// non-empty one.
			ReasoningContent string `json:"reasoning_content"`
			Reasoning        string `json:"reasoning"`
			ReasoningText    string `json:"reasoning_text"`
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
	started   bool
	arguments strings.Builder
}

func parseSSE(ctx context.Context, r io.Reader, onEvent func(durable.StreamEvent) error, provider string) (durable.ModelResponse, error) {
	response := durable.ModelResponse{}
	tools := map[int]*streamTool{}
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
			if choice.FinishReason != nil {
				// pi-ai mapStopReason: other reasons end the answer as an
				// error.
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
			reasoning := choice.Delta.ReasoningContent
			if reasoning == "" {
				reasoning = choice.Delta.Reasoning
			}
			if reasoning == "" {
				reasoning = choice.Delta.ReasoningText
			}
			if reasoning != "" {
				if thinkingIndex < 0 {
					thinkingIndex, blocks = blocks, blocks+1
				}
				response.Thinking += reasoning
				if err := emit(durable.StreamEvent{Type: "thinking_delta", Index: thinkingIndex, Delta: reasoning}); err != nil {
					return err
				}
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
				t := tools[delta.Index]
				if t == nil {
					t = &streamTool{index: blocks}
					blocks++
					tools[delta.Index] = t
				}
				if delta.ID != "" {
					t.id = delta.ID
				}
				if delta.Function.Name != "" {
					t.name += delta.Function.Name
				}
				if !t.started {
					t.started = true
					if err := emit(durable.StreamEvent{Type: "toolcall_start", Index: t.index, ID: t.id, Name: t.name}); err != nil {
						return err
					}
				}
				t.arguments.WriteString(delta.Function.Arguments)
				if delta.Function.Arguments != "" {
					if err := emit(durable.StreamEvent{Type: "toolcall_delta", Index: t.index, Delta: delta.Function.Arguments}); err != nil {
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
		if !gotDone {
			// pi-ai: "Stream ended without finish_reason", a transient ending.
			return durable.ModelResponse{}, sanitizedError("model stream ended before [DONE]", true)
		}
	}
	indices := make([]int, 0, len(tools))
	for i := range tools {
		indices = append(indices, i)
	}
	// Tool indices are normally contiguous, but sort via a small insertion sort
	// to preserve the provider's requested order without another dependency.
	for i := 1; i < len(indices); i++ {
		for j := i; j > 0 && indices[j] < indices[j-1]; j-- {
			indices[j], indices[j-1] = indices[j-1], indices[j]
		}
	}
	for _, i := range indices {
		if err := emit(durable.StreamEvent{Type: "toolcall_end", Index: tools[i].index}); err != nil {
			return durable.ModelResponse{}, err
		}
	}
	for _, i := range indices {
		t := tools[i]
		args := strings.TrimSpace(t.arguments.String())
		if args == "" {
			args = "{}"
		}
		if !json.Valid([]byte(args)) {
			return durable.ModelResponse{}, sanitizedError(fmt.Sprintf("tool call %q has invalid JSON arguments", t.name), false)
		}
		response.ToolCalls = append(response.ToolCalls, durable.ToolCall{ID: t.id, Name: t.name, Arguments: json.RawMessage(args)})
	}
	if finishError != "" {
		return response, classifiedError(finishError, finishError, provider)
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

// compatFields are the request fields pi-ai's openai-completions adapter
// sends by the endpoint it detects from the base URL (detectCompat): store
// false on standard endpoints, the prompt cache key on OpenAI's (or with
// long retention where supported), and the output limit.
func (o OpenAI) compatFields(payload *openAIRequest, req durable.ModelRequest) {
	base := o.BaseURL
	has := func(parts ...string) bool {
		for _, part := range parts {
			if strings.Contains(base, part) {
				return true
			}
		}
		return false
	}
	deepSeek := strings.Contains(strings.ToLower(base), "deepseek.com")
	together := has("api.together.ai", "api.together.xyz")
	nvidia, antLing := has("integrate.api.nvidia.com"), has("api.ant-ling.com")
	cloudflareWorkers, cloudflareGateway := has("api.cloudflare.com"), has("gateway.ai.cloudflare.com")
	zai, moonshot := has("api.z.ai", "open.bigmodel.cn"), has("api.moonshot.")
	nonStandard := nvidia || has("cerebras.ai", "api.x.ai", "chutes.ai", "opencode.ai") || together || deepSeek || zai || moonshot || cloudflareWorkers || cloudflareGateway || antLing
	if !nonStandard {
		off := false
		payload.Store = &off
	}
	retention := ""
	if req.Stream != nil {
		retention = req.Stream.CacheRetention
	}
	if retention == "" {
		retention = "short"
		if os.Getenv("PI_CACHE_RETENTION") == "long" {
			retention = "long"
		}
	}
	longSupported := !(together || cloudflareWorkers || cloudflareGateway || nvidia || antLing)
	if retention != "none" && (strings.Contains(base, "api.openai.com") || retention == "long" && longSupported) && req.SessionID != "" {
		key := []rune(req.SessionID)
		if len(key) > 64 {
			key = key[:64]
		}
		payload.PromptCacheKey = string(key)
	}
	if retention == "long" && longSupported {
		payload.PromptCacheRetention = "24h"
	}
	// OpenAI-style reasoning_effort (pi-ai's default thinking format).
	supportsEffort := !(has("api.x.ai") || zai || moonshot || together || cloudflareGateway || nvidia || antLing)
	if o.Reasoning && supportsEffort {
		if level := o.clampThinkingLevel(req.ThinkingLevel); level != "off" {
			payload.ReasoningEffort = level
			if mapped, ok := o.ThinkingLevelMap[level]; ok && mapped != nil {
				payload.ReasoningEffort = *mapped
			}
		} else if off, ok := o.ThinkingLevelMap["off"]; ok && off != nil {
			payload.ReasoningEffort = *off
		}
	}
	if n := o.outputLimit(req.Messages); n > 0 {
		if deepSeek || has("chutes.ai") || moonshot || cloudflareGateway || together || nvidia || antLing || zai {
			payload.MaxTokens = n
		} else {
			payload.MaxCompletionTokens = n
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
