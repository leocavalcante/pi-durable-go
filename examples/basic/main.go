package main

import (
	"context"
	"fmt"
	"log"

	durable "github.com/leocavalcante/pi-durable-go"
)

type echo struct{}

func (echo) Generate(ctx context.Context, request durable.ModelRequest, delta func(string) error) (durable.ModelResponse, error) {
	if err := ctx.Err(); err != nil {
		return durable.ModelResponse{}, err
	}
	for i := len(request.Messages) - 1; i >= 0; i-- {
		if request.Messages[i].Role == "user" {
			return durable.ModelResponse{Content: "Echo: " + request.Messages[i].Content}, nil
		}
	}
	return durable.ModelResponse{Content: "Hello"}, nil
}
func main() {
	ctx := context.Background()
	h, err := durable.Open(ctx, durable.NewMemoryStorage(), durable.HarnessOptions{Model: echo{}})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := h.Close(ctx); err != nil {
			log.Print(err)
		}
	}()
	root, err := h.Root(ctx, durable.ConversationOptions{Agent: durable.Agent{Model: durable.ModelRef{Provider: "fake", ID: "echo"}}})
	if err != nil {
		log.Fatal(err)
	}
	sub, err := root.Submit(ctx, durable.SubmissionRequest{Content: "Durable agents in Go", RequestID: "example-1"})
	if err != nil {
		log.Fatal(err)
	}
	result, err := sub.Wait(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if result.Status != "done" {
		log.Fatal(result.Reason)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(state.Entries[result.Answer].Message.Content)
}
