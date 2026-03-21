package llm

import (
	"context"
	"fmt"
	"strings"
)

// LLMClient is the interface all LLM providers must implement.
// The inference pipeline depends only on this interface.
type LLMClient interface {
	// GenerateStructured sends prompt to the LLM and returns the response
	// body unmarshalled into result. schema is a JSON Schema describing
	// the expected response shape. The implementation is responsible for
	// forcing structured output using provider-appropriate mechanisms
	// (tool-use forcing for Anthropic, response_format for OpenAI).
	// Retries once on malformed output before returning an error.
	GenerateStructured(ctx context.Context, prompt string, schema []byte, result any) error

	// Provider returns a string identifying the provider (for logging).
	Provider() string
}

// ProviderFromString returns an LLMClient for the named provider.
// Valid values: "anthropic", "openai".
// Returns an error for unknown providers.
func ProviderFromString(provider string) (LLMClient, error) {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "anthropic":
		return NewAnthropicClient()
	case "openai":
		return NewOpenAIClient()
	default:
		return nil, fmt.Errorf("unknown llm provider: %s", provider)
	}
}
