package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	openai "github.com/openai/openai-go"
)

const defaultOpenAIModel = "gpt-4o"

// OpenAIClient is the OpenAI implementation of LLMClient.
type OpenAIClient struct {
	client openai.Client
	model  string
}

func NewOpenAIClient() (*OpenAIClient, error) {
	model := os.Getenv("STRATA_OPENAI_MODEL")
	if model == "" {
		model = defaultOpenAIModel
	}

	client := openai.NewClient()
	return &OpenAIClient{client: client, model: model}, nil
}

// GenerateStructured sends a structured prompt to OpenAI.
func (c *OpenAIClient) GenerateStructured(ctx context.Context, prompt string, schema []byte, result any) (GenerateResult, error) {
	if os.Getenv("OPENAI_API_KEY") == "" {
		return GenerateResult{}, fmt.Errorf("openai: missing OPENAI_API_KEY")
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
