package overlay

import (
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// AppendCorrection appends a single correction to corrections.yaml.
// This function is append-only. It MUST NOT modify or delete existing records.
// If the file does not exist, it creates it with the correct smif_version header.
func AppendCorrection(path string, smifVersion string, c Correction) error {
	if strings.TrimSpace(c.CorrectionID) == "" {
		u := strings.ReplaceAll(uuid.NewString(), "-", "")
		if len(u) > 8 {
			u = u[:8]
		}
		c.CorrectionID = "corr_" + u
	}

	item, err := marshalCorrectionItem(c)
	if err != nil {
		return fmt.Errorf("marshal correction: %w", err)
	}

	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			content := fmt.Sprintf("smif_version: %q\ncorrections:\n%s", smifVersion, item)
			return os.WriteFile(path, []byte(content), 0o644)
		}
		return fmt.Errorf("stat corrections file: %w", err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open corrections file for append: %w", err)
	}
	defer f.Close()

	if _, err := f.WriteString("\n" + item); err != nil {
		return fmt.Errorf("append correction: %w", err)
	}

	return nil
}

func marshalCorrectionItem(c Correction) (string, error) {
	b, err := yaml.Marshal(c)
	if err != nil {
		return "", err
	}

	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) == 0 {
		return "", fmt.Errorf("empty marshaled correction")
	}

	var sb strings.Builder
	sb.WriteString("  - ")
	sb.WriteString(lines[0])
	sb.WriteString("\n")
	for i := 1; i < len(lines); i++ {
		sb.WriteString("    ")
		sb.WriteString(lines[i])
		sb.WriteString("\n")
	}

	return sb.String(), nil
}
