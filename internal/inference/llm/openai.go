package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	openai "github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
)

const defaultOpenAIModel = "gpt-4o"

// OpenAIClient is the OpenAI implementation of LLMClient.
// It also supports any OpenAI-compatible endpoint (DeepSeek, Gemini, Ollama, etc.)
// via a custom base URL.
type OpenAIClient struct {
	client openai.Client
	model  string
	apiKey string
}

func NewOpenAIClient() (*OpenAIClient, error) {
	// STRATA_LLM_MODEL takes precedence; fall back to the legacy
	// STRATA_OPENAI_MODEL override, then the built-in default.
	model := os.Getenv("STRATA_LLM_MODEL")
	if model == "" {
		model = envOrDefault("STRATA_OPENAI_MODEL", defaultOpenAIModel)
	}

	// STRATA_LLM_API_KEY takes precedence; fall back to OPENAI_API_KEY (read
	// automatically by the SDK).
	apiKey := os.Getenv("STRATA_LLM_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}

	opts := []option.RequestOption{}
	if apiKey != "" {
		opts = append(opts, option.WithAPIKey(apiKey))
	}

	client := openai.NewClient(opts...)
	return &OpenAIClient{client: client, model: model, apiKey: apiKey}, nil
}

// NewOpenAIClientWithOptions creates an OpenAI-compatible client with explicit
// model, base URL, and API key. Used when STRATA_LLM_BASE_URL is set, enabling
// providers such as DeepSeek, Gemini, and local Ollama.
func NewOpenAIClientWithOptions(model, baseURL, apiKey string) (*OpenAIClient, error) {
	if model == "" {
		model = DefaultModel
	}

	opts := []option.RequestOption{}
	if baseURL != "" {
		opts = append(opts, option.WithBaseURL(baseURL))
	}
	if apiKey != "" {
		opts = append(opts, option.WithAPIKey(apiKey))
	}

	client := openai.NewClient(opts...)
	return &OpenAIClient{client: client, model: model, apiKey: apiKey}, nil
}

// GenerateStructured sends a structured prompt to the OpenAI-compatible endpoint.
func (c *OpenAIClient) GenerateStructured(ctx context.Context, prompt string, schema []byte, result any) (GenerateResult, error) {
	if c.apiKey == "" {
		return GenerateResult{}, fmt.Errorf("openai: missing API key (set STRATA_LLM_API_KEY or OPENAI_API_KEY)")
	}

	var schemaObj map[string]any
	if err := json.Unmarshal(schema, &schemaObj); err != nil {
		return GenerateResult{}, fmt.Errorf("openai: parse json schema: %w", err)
	}

	request := openai.ChatCompletionNewParams{
		Model:               openai.ChatModel(c.model),
		Messages:            []openai.ChatCompletionMessageParamUnion{openai.UserMessage(prompt)},
		MaxCompletionTokens: openai.Int(maxTokens),
		ResponseFormat: openai.ChatCompletionNewParamsResponseFormatUnion{
			OfJSONSchema: &openai.ResponseFormatJSONSchemaParam{
				JSONSchema: openai.ResponseFormatJSONSchemaJSONSchemaParam{
					Name:   "structured_output",
					Strict: openai.Bool(true),
					Schema: schemaObj,
				},
			},
		},
	}

	for attempt := 0; attempt < 2; attempt++ {
		resp, err := c.client.Chat.Completions.New(ctx, request)
		if err != nil {
			return GenerateResult{}, fmt.Errorf("openai: api call failed: %w", err)
		}

		if len(resp.Choices) == 0 {
			if attempt == 1 {
				return GenerateResult{}, fmt.Errorf("openai: malformed structured output: empty choices")
			}
			continue
		}

		payload := resp.Choices[0].Message.Content
		if payload == "" {
			if attempt == 1 {
				return GenerateResult{}, fmt.Errorf("openai: malformed structured output: empty content")
			}
			continue
		}

		if err := json.Unmarshal([]byte(payload), result); err != nil {
			if attempt == 1 {
				return GenerateResult{}, fmt.Errorf("openai: malformed structured output: %w", err)
			}
			continue
		}

		return GenerateResult{
			TokensIn:  int(resp.Usage.PromptTokens),
			TokensOut: int(resp.Usage.CompletionTokens),
		}, nil
	}

	return GenerateResult{}, fmt.Errorf("openai: malformed structured output")
}

// Provider returns the provider identifier.
func (c *OpenAIClient) Provider() string {
	return "openai"
}

// Model returns the model name used for inference calls.
func (c *OpenAIClient) Model() string {
	return c.model
}
