// Package model contains model adapters. OpenAI implements the common
// OpenAI-compatible Chat Completions streaming protocol, not pi-ai's full
// provider system.
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
}

func (o OpenAI) Generate(ctx context.Context, req durable.ModelRequest, onDelta func(string) error) (durable.ModelResponse, error) {
	endpoint, err := completionsURL(o.BaseURL)
	if err != nil {
		return durable.ModelResponse{}, err
	}
	payload := openAIRequest{Model: req.Model.ID, Messages: make([]openAIMessage, 0, len(req.Messages)), Stream: true, StreamOptions: &streamOptions{IncludeUsage: true}}
	for _, m := range req.Messages {
		role := m.Role
		switch role {
		case "user", "assistant", "system", "developer", "tool":
		default:
			return durable.ModelResponse{}, fmt.Errorf("unsupported message role %q", role)
		}
		msg := openAIMessage{Role: role, Content: m.Content}
		if role == "tool" {
			msg.ToolCallID = m.ToolCallID
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
	if len(req.Tools) > 0 {
		payload.Tools = make([]openAITool, 0, len(req.Tools))
		for _, tool := range req.Tools {
			var parameters any
			if len(tool.Parameters) == 0 || !json.Valid(tool.Parameters) {
				return durable.ModelResponse{}, fmt.Errorf("invalid parameters schema for tool %q", tool.Name)
			}
			if err := json.Unmarshal(tool.Parameters, &parameters); err != nil {
				return durable.ModelResponse{}, fmt.Errorf("invalid parameters schema for tool %q", tool.Name)
			}
			payload.Tools = append(payload.Tools, openAITool{Type: "function", Function: openAIFunctionDefinition{Name: tool.Name, Description: tool.Description, Parameters: parameters}})
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return durable.ModelResponse{}, fmt.Errorf("encode model request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return durable.ModelResponse{}, fmt.Errorf("create model request: %w", err)
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
		return durable.ModelResponse{}, errors.New("model request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return durable.ModelResponse{}, fmt.Errorf("model endpoint returned HTTP %d", resp.StatusCode)
	}
	maxBytes := o.MaxResponseBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxResponseBytes
	}
	limited := &limitedReader{r: resp.Body, remaining: maxBytes}
	response, err := parseSSE(ctx, limited, onDelta)
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
	Role       string           `json:"role"`
	Content    string           `json:"content,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	ToolCalls  []openAIToolCall `json:"tool_calls,omitempty"`
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

var errResponseTooLarge = errors.New("model response exceeds configured size limit")

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

func parseSSE(ctx context.Context, r io.Reader, onDelta func(string) error) (durable.ModelResponse, error) {
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
			return errors.New("invalid event in model stream")
		}
		if len(chunk.Error) > 0 && string(chunk.Error) != "null" {
			return errors.New("model stream reported an error")
		}
		if chunk.Usage != nil {
			response.Usage.InputTokens = chunk.Usage.PromptTokens
			response.Usage.OutputTokens = chunk.Usage.CompletionTokens
		}
		for _, choice := range chunk.Choices {
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
		return durable.ModelResponse{}, errors.New("model stream read failed or exceeded line limit")
	}
	if !gotDone {
		if err := process(); err != nil {
			return durable.ModelResponse{}, err
		}
		if !gotDone {
			return durable.ModelResponse{}, errors.New("model stream ended before [DONE]")
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
			return durable.ModelResponse{}, fmt.Errorf("tool call %q has invalid JSON arguments", t.name)
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
		return "", errors.New("invalid model BaseURL")
	}
	path := strings.TrimRight(u.Path, "/")
	if !strings.HasSuffix(path, "/chat/completions") {
		path += "/chat/completions"
	}
	u.Path = path
	return u.String(), nil
}
