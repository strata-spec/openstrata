package llm

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// DefaultModel is the default LLM model used when no override is configured.
const DefaultModel = "claude-sonnet-4-20250514"

// LLMClient is the interface all LLM providers must implement.
// The inference pipeline depends only on this interface.
type LLMClient interface {
	// GenerateStructured sends prompt to the LLM and returns the response
	// body unmarshalled into result. schema is a JSON Schema describing
	// the expected response shape. The implementation is responsible for
	// forcing structured output using provider-appropriate mechanisms
	// (tool-use forcing for Anthropic, response_format for OpenAI).
	// Retries once on malformed output before returning an error.
	GenerateStructured(ctx context.Context, prompt string, schema []byte, result any) (GenerateResult, error)

	// Provider returns a string identifying the provider (for logging).
	Provider() string

	// Model returns the model name used for inference calls.
	Model() string
}

// GenerateResult contains metadata returned by a structured generation call.
type GenerateResult struct {
	TokensIn  int
	TokensOut int
}

// EnvConfig holds the resolved LLM configuration read from environment variables.
type EnvConfig struct {
	Model   string
	BaseURL string
	APIKey  string
}

// ReadEnvConfig reads LLM configuration from environment variables.
//
// Precedence:
//
//	STRATA_LLM_MODEL    – model name passed to the API (default: DefaultModel)
//	STRATA_LLM_BASE_URL – optional OpenAI-compatible base URL override (default: "")
//	STRATA_LLM_API_KEY  – API key; falls back to ANTHROPIC_API_KEY then OPENAI_API_KEY
func ReadEnvConfig() EnvConfig {
	apiKey := os.Getenv("STRATA_LLM_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("ANTHROPIC_API_KEY")
	}
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	return EnvConfig{
		Model:   envOrDefault("STRATA_LLM_MODEL", DefaultModel),
		BaseURL: os.Getenv("STRATA_LLM_BASE_URL"),
		APIKey:  apiKey,
	}
}

// envOrDefault returns the value of the named environment variable, or fallback
// if the variable is unset or empty.
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// NewClientFromEnv creates an LLMClient using environment variables and an
// optional provider hint (e.g. the value of --llm).
//
// If STRATA_LLM_BASE_URL is set, an OpenAI-compatible client is returned
// regardless of providerHint, enabling use of DeepSeek, Gemini, local Ollama,
// etc. with no code changes.
//
// Otherwise providerHint selects between "anthropic" (default) and "openai".
func NewClientFromEnv(providerHint string) (LLMClient, error) {
	cfg := ReadEnvConfig()
	if cfg.BaseURL != "" {
		return NewOpenAIClientWithOptions(cfg.Model, cfg.BaseURL, cfg.APIKey)
	}
	return ProviderFromString(providerHint)
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
