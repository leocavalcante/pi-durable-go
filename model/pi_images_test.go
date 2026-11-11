package model

import (
	"context"
	"encoding/json"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
)

func TestPiBridgeSendsImageBlocksAndReadsImageInput(t *testing.T) {
	p := fakePi(t)
	png := durable.ImageContent{MimeType: "image/png", Data: "iVBORw0KGgo="}
	req := piRequest("inspect")
	req.Messages = []durable.Message{
		{Role: "user", Content: "what is this?", Images: []durable.ImageContent{png}},
		{Role: "tool", ToolCallID: "call-1", ToolName: "read", Images: []durable.ImageContent{png}},
	}
	inspect := inspectPi(t, p, req)
	messages := inspect.Context.Messages
	if len(messages) != 2 {
		t.Fatalf("messages=%+v", messages)
	}
	var user, tool []map[string]any
	if err := json.Unmarshal(messages[0].Content, &user); err != nil {
		t.Fatalf("user content=%s: %v", messages[0].Content, err)
	}
	if err := json.Unmarshal(messages[1].Content, &tool); err != nil {
		t.Fatalf("tool content=%s: %v", messages[1].Content, err)
	}
	if len(user) != 2 || user[0]["text"] != "what is this?" || user[1]["type"] != "image" || user[1]["data"] != png.Data || user[1]["mimeType"] != png.MimeType {
		t.Fatalf("user blocks=%v", user)
	}
	if len(tool) != 1 || tool[0]["type"] != "image" {
		t.Fatalf("tool blocks=%v", tool)
	}
	if !p.AcceptsImages(durable.ModelRef{Provider: "fake", ID: "vision"}) || p.AcceptsImages(durable.ModelRef{Provider: "fake", ID: "text-only"}) {
		t.Fatal("image input not read from the catalog")
	}
	if got := p.ImageLimits(durable.ModelRef{Provider: "fake", ID: "small-images"}); got != (durable.ImageLimits{MaxWidth: 512, MaxHeight: 256, MaxBytes: 1000}) {
		t.Fatalf("image limits=%+v", got)
	}
}

func TestPiBridgePassesTheIdentity(t *testing.T) {
	p := fakePi(t)
	req := piRequest("inspect")
	req.Identity = &durable.ClientIdentity{UserAgent: json.RawMessage(`"short"`), Version: "9.9"}
	response, err := p.Generate(context.Background(), req, nil)
	if err != nil {
		t.Fatal(err)
	}
	var inspect struct {
		Options struct {
			Identity map[string]any `json:"identity"`
		} `json:"options"`
	}
	if err := json.Unmarshal([]byte(response.Content), &inspect); err != nil {
		t.Fatal(err)
	}
	if inspect.Options.Identity["userAgent"] != "short" || inspect.Options.Identity["version"] != "9.9" {
		t.Fatalf("identity=%v", inspect.Options.Identity)
	}
}
