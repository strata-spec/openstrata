package inference

import (
	"fmt"
	"os"
)

const maxStrataMDBytes = 16000
const strataMDTruncationWarning = "\n[strata.md truncated: file exceeds 4000 token limit]"

// Load reads the strata.md file at path and returns its content as a
// context string for injection into LLM prompts.
// If the file does not exist, returns ("", false, nil) — absence is not an error.
// If the file exists but cannot be read, returns a wrapped error.
// If the content exceeds 4000 tokens (approximated as 16000 bytes),
// it is chunked: only the first 16000 bytes are used.
func Load(path string) (content string, found bool, err error) {
	b, readErr := os.ReadFile(path)
	if readErr != nil {
		if os.IsNotExist(readErr) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("load strata.md: %w", readErr)
	}

	if len(b) > maxStrataMDBytes {
		return string(b[:maxStrataMDBytes]) + strataMDTruncationWarning, true, nil
	}

	return string(b), true, nil
}
