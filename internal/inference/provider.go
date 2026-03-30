package inference

import (
	"github.com/strata-spec/openstrata/internal/inference/llm"
)

// ProviderFromString resolves an LLM provider name to a configured client.
// Environment variables STRATA_LLM_MODEL, STRATA_LLM_BASE_URL, and
// STRATA_LLM_API_KEY are applied automatically. If STRATA_LLM_BASE_URL is
// set, an OpenAI-compatible client is returned regardless of provider.
func ProviderFromString(provider string) (llm.LLMClient, error) {
	return llm.NewClientFromEnv(provider)
}
