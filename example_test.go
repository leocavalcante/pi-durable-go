package durable_test

import (
	"context"
	"fmt"
	"log"

	durable "github.com/leocavalcante/pi-durable-go"
)

// echo answers with the user's last message.
type echo struct{}

func (echo) Generate(ctx context.Context, req durable.ModelRequest, _ func(string) error) (durable.ModelResponse, error) {
	last := req.Messages[len(req.Messages)-1]
	return durable.ModelResponse{Content: "Echo: " + last.Content}, nil
}

// A conversation over memory storage, with a model that echoes.
func Example() {
	ctx := context.Background()
	h, err := durable.Open(ctx, durable.NewMemoryStorage(), durable.HarnessOptions{Model: echo{}})
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close(ctx)

	root, err := h.Root(ctx, durable.ConversationOptions{
		Agent: durable.Agent{Model: durable.ModelRef{Provider: "example", ID: "echo"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	submission, err := root.Submit(ctx, durable.SubmissionRequest{Content: "Hello", RequestID: "hello-1"})
	if err != nil {
		log.Fatal(err)
	}
	result, err := submission.Wait(ctx)
	if err != nil {
		log.Fatal(err)
	}
	entries, err := root.Entries(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, entry := range entries {
		if entry.ID == result.Answer {
			fmt.Println(result.Status, entry.Message.Content)
		}
	}
	// Output: done Echo: Hello
}
