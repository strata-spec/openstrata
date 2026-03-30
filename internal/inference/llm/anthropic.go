package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	anthropic "github.com/anthropics/anthropic-sdk-go"
)

const maxTokens = 4096

// AnthropicClient is the Anthropic implementation of LLMClient.
type AnthropicClient struct {
	client anthropic.Client
	model  string
	apiKey string
}

func NewAnthropicClient() (*AnthropicClient, error) {
	// STRATA_LLM_MODEL takes precedence; fall back to the legacy
	// STRATA_ANTHROPIC_MODEL override, then the built-in default.
	model := os.Getenv("STRATA_LLM_MODEL")
	if model == "" {
		model = envOrDefault("STRATA_ANTHROPIC_MODEL", DefaultModel)
	}

	// STRATA_LLM_API_KEY takes precedence; fall back to ANTHROPIC_API_KEY.
	apiKey := os.Getenv("STRATA_LLM_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("ANTHROPIC_API_KEY")
	}

	client := anthropic.NewClient()
	return &AnthropicClient{client: client, model: model, apiKey: apiKey}, nil
}

// GenerateStructured sends a structured prompt to Anthropic.
func (c *AnthropicClient) GenerateStructured(ctx context.Context, prompt string, schema []byte, result any) (GenerateResult, error) {
	if c.apiKey == "" {
		return GenerateResult{}, fmt.Errorf("anthropic: missing API key (set STRATA_LLM_API_KEY or ANTHROPIC_API_KEY)")
	}

	var schemaMap map[string]any
	if err := json.Unmarshal(schema, &schemaMap); err != nil {
		return GenerateResult{}, fmt.Errorf("anthropic: parse json schema: %w", err)
	}

	inputSchema := anthropic.ToolInputSchemaParam{ExtraFields: schemaMap}
	request := anthropic.MessageNewParams{
		Model:     anthropic.Model(c.model),
		MaxTokens: maxTokens,
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(prompt)),
		},
		Tools: []anthropic.ToolUnionParam{
			anthropic.ToolUnionParamOfTool(inputSchema, "structured_output"),
		},
		ToolChoice: anthropic.ToolChoiceParamOfTool("structured_output"),
	}

	for attempt := 0; attempt < 2; attempt++ {
		resp, err := c.client.Messages.New(ctx, request)
		if err != nil {
			return GenerateResult{}, fmt.Errorf("anthropic: api call failed: %w", err)
		}

		parseErr := extractAnthropicToolResult(resp, result)
		if parseErr == nil {
			return GenerateResult{
				TokensIn:  int(resp.Usage.InputTokens),
				TokensOut: int(resp.Usage.OutputTokens),
			}, nil
		}
		if attempt == 1 {
			return GenerateResult{}, fmt.Errorf("anthropic: malformed structured output: %w", parseErr)
		}
	}

	return GenerateResult{}, fmt.Errorf("anthropic: malformed structured output")
}

// Provider returns the provider identifier.
func (c *AnthropicClient) Provider() string {
	return "anthropic"
}

// Model returns the model name used for inference calls.
func (c *AnthropicClient) Model() string {
	return c.model
}

func extractAnthropicToolResult(resp *anthropic.Message, result any) error {
	if resp == nil {
		return fmt.Errorf("empty response")
	}

	for _, block := range resp.Content {
		if block.Type != "tool_use" {
			continue
		}

		tool := block.AsToolUse()
		if tool.Name != "structured_output" {
			continue
		}

		if len(tool.Input) == 0 {
			return fmt.Errorf("empty tool input")
		}
		if err := json.Unmarshal(tool.Input, result); err != nil {
			return err
		}
		return nil
	}

	return fmt.Errorf("structured_output tool block not found")
}
