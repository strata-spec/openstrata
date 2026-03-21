package inference

import (
	"github.com/strata-spec/openstrata/internal/inference/llm"
)

// ProviderFromString resolves an LLM provider name to a configured client.
func ProviderFromString(provider string) (llm.LLMClient, error) {
	return llm.ProviderFromString(provider)
}
