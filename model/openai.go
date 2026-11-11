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
	"net/http"
	"net/url"
	"strings"

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
	endpoint, err := completionsURL(o.BaseURL)
	if err != nil {
		return durable.ModelResponse{}, err
	}
	payload := openAIRequest{Model: req.Model.ID, Messages: make([]openAIMessage, 0, len(req.Messages)), Stream: true, StreamOptions: &streamOptions{IncludeUsage: true}}
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
	for _, m := range req.Messages {
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
			var parts []openAIContentPart
			if m.Content != "" {
				parts = append(parts, openAIContentPart{Type: "text", Text: m.Content})
			}
			if o.NoImages {
				parts = append(parts, openAIContentPart{Type: "text", Text: userImagePlaceholder})
			} else {
				for _, image := range m.Images {
					parts = append(parts, imageURL(image))
				}
			}
			msg.Content = parts
		case role == "tool":
			msg.ToolCallID = m.ToolCallID
			text := m.Content
			if len(m.Images) > 0 && o.NoImages {
				if text != "" {
					text += "\n"
				}
				text += toolImagePlaceholder
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
	if len(req.Tools) > 0 {
		payload.Tools = make([]openAITool, 0, len(req.Tools))
		for _, tool := range req.Tools {
			var parameters any
			if len(tool.Parameters) == 0 || !json.Valid(tool.Parameters) {
				return durable.ModelResponse{}, sanitizedError(fmt.Sprintf("invalid parameters schema for tool %q", tool.Name), false)
			}
			if err := json.Unmarshal(tool.Parameters, &parameters); err != nil {
				return durable.ModelResponse{}, sanitizedError(fmt.Sprintf("invalid parameters schema for tool %q", tool.Name), false)
			}
			payload.Tools = append(payload.Tools, openAITool{Type: "function", Function: openAIFunctionDefinition{Name: tool.Name, Description: tool.Description, Parameters: parameters}})
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return durable.ModelResponse{}, sanitizedError("cannot encode model request", false)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return durable.ModelResponse{}, sanitizedError("cannot create model request", false)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
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
	response, err := parseSSE(ctx, limited, onDelta, req.Model.Provider)
	if err != nil {
		return durable.ModelResponse{}, err
	}
	return response, nil
}

type openAIRequest struct {
	Model         string          `json:"model"`
	Messages      []openAIMessage `json:"messages"`
	Tools         []openAITool    `json:"tools,omitempty"`
	Stream        bool            `json:"stream"`
	StreamOptions *streamOptions  `json:"stream_options,omitempty"`
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
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters"`
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
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
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
	arguments strings.Builder
}

func parseSSE(ctx context.Context, r io.Reader, onDelta func(string) error, provider string) (durable.ModelResponse, error) {
	response := durable.ModelResponse{}
	tools := map[int]*streamTool{}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), maxSSELineBytes)
	var data []string
	gotDone := false
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
		if chunk.Usage != nil {
			response.Usage.InputTokens = chunk.Usage.PromptTokens
			response.Usage.OutputTokens = chunk.Usage.CompletionTokens
		}
		for _, choice := range chunk.Choices {
			if choice.FinishReason != nil {
				switch *choice.FinishReason {
				case "length":
					response.StopReason = "length"
				case "tool_calls", "function_call":
					response.StopReason = "toolUse"
				case "stop":
					response.StopReason = "stop"
				}
			}
			if choice.Delta.Content != "" {
				response.Content += choice.Delta.Content
				if onDelta != nil {
					if err := onDelta(choice.Delta.Content); err != nil {
						return err
					}
				}
			}
			for _, delta := range choice.Delta.ToolCalls {
				t := tools[delta.Index]
				if t == nil {
					t = &streamTool{}
					tools[delta.Index] = t
				}
				if delta.ID != "" {
					t.id = delta.ID
				}
				if delta.Function.Name != "" {
					t.name += delta.Function.Name
				}
				t.arguments.WriteString(delta.Function.Arguments)
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
