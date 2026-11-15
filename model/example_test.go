package model_test

import (
	"fmt"

	"github.com/leocavalcante/pi-durable-go/model"
)

// A model from pi-ai's catalog, configured as pi-ai defines it; set its API
// key to use it.
func ExampleOpenAIModel() {
	llm, ok := model.OpenAIModel("groq", "llama-3.3-70b-versatile")
	fmt.Println(ok, llm.BaseURL, llm.Window, llm.MaxTokens)
	// Output: true https://api.groq.com/openai/v1 131072 32768
}
