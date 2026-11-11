// Chat makes real provider requests. Run only with a model/provider you intend to use.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/model"
	"github.com/leocavalcante/pi-durable-go/storage"
	"github.com/leocavalcante/pi-durable-go/tools"
)

func main() {
	modelID := flag.String("model", "", "model ID (required)")
	provider := flag.String("provider", "openai", "Pi provider ID")
	adapter := flag.String("adapter", "pi", "model adapter: pi (reuse Pi auth) or openai (pure Go)")
	piPackage := flag.String("pi-package-dir", "", "installed pi-coding-agent package directory; empty discovers Pi")
	piAgent := flag.String("pi-agent-dir", "", "Pi auth/models directory; empty uses Pi's default")
	baseURL := flag.String("base-url", "https://api.openai.com/v1", "API base URL for -adapter openai only")
	stateDir := flag.String("state", ".pi-durable", "private state directory")
	prompt := flag.String("prompt", "", "input prompt; empty resumes existing work")
	coding := flag.Bool("coding-tools", false, "enable local read/write/edit/bash; not sandboxed")
	requestID := flag.String("request-id", "", "idempotent input admission key")
	flag.Parse()
	if *modelID == "" {
		log.Fatal("-model is required")
	}
	var generator durable.Model
	switch *adapter {
	case "pi":
		generator = model.Pi{PackageDir: *piPackage, AgentDir: *piAgent}
	case "openai":
		generator = model.OpenAI{BaseURL: *baseURL, APIKey: os.Getenv("OPENAI_API_KEY")}
	default:
		log.Fatal("-adapter must be pi or openai")
	}
	if *adapter != "openai" && *baseURL != "https://api.openai.com/v1" {
		log.Fatal("-base-url applies only to -adapter openai; configure Pi models.json for -adapter pi")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := os.MkdirAll(*stateDir, 0700); err != nil {
		log.Fatal(err)
	}
	store, err := storage.OpenSQLite(ctx, filepath.Join(*stateDir, "session.sqlite"))
	if err != nil {
		log.Fatal(err)
	}
	registry := durable.NewRegistry()
	opts := durable.HarnessOptions{Model: generator, Registry: registry}
	if *coding {
		if err := registry.Install(tools.Coding()); err != nil {
			log.Fatal(err)
		}
		cwd, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		opts.Environment = func(c durable.ConversationRecord) (durable.ExecutionEnv, error) {
			dir := c.Agent.Cwd
			if dir == "" {
				dir = cwd
			}
			return tools.NewLocalEnv(dir)
		}
	}
	h, err := durable.Open(ctx, store, opts)
	if err != nil {
		store.Close()
		log.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := h.Close(closeCtx); err != nil {
			log.Print(err)
		}
	}()
	root, err := h.Root(ctx, durable.ConversationOptions{Agent: durable.Agent{Model: durable.ModelRef{Provider: *provider, ID: *modelID}}})
	if err != nil {
		log.Fatal(err)
	}
	h.Resume()
	if *prompt == "" {
		fmt.Println("Existing work resumed. Press Ctrl+C to stop the host.")
		<-ctx.Done()
		return
	}
	sub, err := root.Submit(ctx, durable.SubmissionRequest{Content: *prompt, RequestID: *requestID})
	if err != nil {
		log.Fatal(err)
	}
	settled, err := sub.Wait(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if settled.Status != "done" {
		log.Fatal(settled.Reason)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(state.Entries[settled.Answer].Message.Content)
}
