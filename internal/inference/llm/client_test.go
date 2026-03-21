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
