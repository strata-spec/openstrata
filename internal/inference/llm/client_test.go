package llm

import "testing"

func TestProviderFromString(t *testing.T) {
	t.Parallel()

	client, err := ProviderFromString("anthropic")
	if err != nil {
		t.Fatalf("ProviderFromString(anthropic) returned error: %v", err)
	}
	if client == nil {
		t.Fatalf("expected non-nil anthropic client")
	}

	client, err = ProviderFromString("openai")
	if err != nil {
		t.Fatalf("ProviderFromString(openai) returned error: %v", err)
	}
	if client == nil {
		t.Fatalf("expected non-nil openai client")
	}

	client, err = ProviderFromString("unknown")
	if err == nil {
		t.Fatalf("expected error for unknown provider")
	}
	if client != nil {
		t.Fatalf("expected nil client for unknown provider")
	}
}

func TestEnvLLMModelOverride(t *testing.T) {
	t.Setenv("STRATA_LLM_MODEL", "deepseek-chat")
	t.Setenv("STRATA_LLM_BASE_URL", "https://api.deepseek.com")
	t.Setenv("STRATA_LLM_API_KEY", "sk-test")

	cfg := ReadEnvConfig()

	if cfg.Model != "deepseek-chat" {
		t.Fatalf("expected model deepseek-chat, got %q", cfg.Model)
	}
	if cfg.BaseURL != "https://api.deepseek.com" {
		t.Fatalf("expected base_url https://api.deepseek.com, got %q", cfg.BaseURL)
	}
	if cfg.APIKey != "sk-test" {
		t.Fatalf("expected apiKey sk-test, got %q", cfg.APIKey)
	}

	// NewClientFromEnv must return an OpenAI-compatible client when BaseURL is set.
	client, err := NewClientFromEnv("anthropic")
	if err != nil {
		t.Fatalf("NewClientFromEnv returned error: %v", err)
	}
	if client.Model() != "deepseek-chat" {
		t.Fatalf("expected client model deepseek-chat, got %q", client.Model())
	}
	if client.Provider() != "openai" {
		t.Fatalf("expected openai provider for OpenAI-compatible client, got %q", client.Provider())
	}
}

func TestEnvLLMModelDefault(t *testing.T) {
	// Ensure that when no override is set the default model is used.
	t.Setenv("STRATA_LLM_MODEL", "")
	t.Setenv("STRATA_LLM_BASE_URL", "")
	t.Setenv("STRATA_LLM_API_KEY", "")
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("OPENAI_API_KEY", "")

	cfg := ReadEnvConfig()

	if cfg.Model != DefaultModel {
		t.Fatalf("expected default model %q, got %q", DefaultModel, cfg.Model)
	}
	if cfg.BaseURL != "" {
		t.Fatalf("expected empty base_url, got %q", cfg.BaseURL)
	}
}

func TestEnvLLMAPIKeyFallback(t *testing.T) {
	// STRATA_LLM_API_KEY → ANTHROPIC_API_KEY → OPENAI_API_KEY
	t.Setenv("STRATA_LLM_API_KEY", "")
	t.Setenv("ANTHROPIC_API_KEY", "anthropic-key")
	t.Setenv("OPENAI_API_KEY", "openai-key")

	cfg := ReadEnvConfig()
	if cfg.APIKey != "anthropic-key" {
		t.Fatalf("expected ANTHROPIC_API_KEY fallback, got %q", cfg.APIKey)
	}

	// When ANTHROPIC_API_KEY is also empty, fall back to OPENAI_API_KEY.
	t.Setenv("ANTHROPIC_API_KEY", "")
	cfg = ReadEnvConfig()
	if cfg.APIKey != "openai-key" {
		t.Fatalf("expected OPENAI_API_KEY fallback, got %q", cfg.APIKey)
	}
}
