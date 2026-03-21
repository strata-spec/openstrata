package smif

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/strata-spec/openstrata/internal/postgres"
)

// Compute returns a SHA256 fingerprint of the model's DDL structure.
// Input: sorted list of "name:type:nullable" per column + constraint names.
// This is the value stored in model.ddl_fingerprint.
func Compute(table postgres.TableInfo) string {
	parts := make([]string, 0, len(table.Columns)+1+len(table.ForeignKeys))
	for _, c := range table.Columns {
		parts = append(parts, fmt.Sprintf("%s:%s:%t", c.Name, c.DataType, c.IsNullable))
	}
	if len(table.PrimaryKey) > 0 {
		pk := append([]string(nil), table.PrimaryKey...)
		sort.Strings(pk)
		parts = append(parts, "pk:"+strings.Join(pk, ","))
	}
	for _, fk := range table.ForeignKeys {
		parts = append(parts, "fk:"+fk.ConstraintName)
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return "sha256:" + hex.EncodeToString(sum[:])
}
