package smif

import (
	"testing"
	"time"
)

func TestValidateMustRules(t *testing.T) {
	tests := []struct {
		name string
		rule string
		pass ValidationDoc
		fail ValidationDoc
	}{
		{
			name: "V-001 smif_version semver",
			rule: "V-001",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.SMIFVersion = "1"
				return m
			}()},
		},
		{
			name: "V-002 generated_at rfc3339",
			rule: "V-002",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.GeneratedAt = "not-a-time"
				return m
			}()},
		},
		{
			name: "V-007 models non-empty",
			rule: "V-007",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.Models = nil
				return m
			}()},
		},
		{
			name: "V-009 model_id unique",
			rule: "V-009",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.Models[1].ModelID = m.Models[0].ModelID
				return m
			}()},
		},
		{
			name: "V-012 column names unique",
			rule: "V-012",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.Models[0].Columns = append(m.Models[0].Columns, m.Models[0].Columns[0])
				return m
			}()},
		},
		{
			name: "V-014 valid column role",
			rule: "V-014",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.Models[0].Columns[0].Role = "invalid_role"
				return m
			}()},
		},
		{
			name: "V-022 preferred relationship uniqueness",
			rule: "V-022",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.Relationships = append(m.Relationships, Relationship{
					RelationshipID:   "rel_orders_users_2",
					FromModel:        "users",
					FromColumn:       "id",
					ToModel:          "orders",
					ToColumn:         "user_id",
					RelationshipType: "one_to_many",
					JoinCondition:    "users.id = orders.user_id",
					Preferred:        true,
					Provenance:       baseProvenance(),
				})
				return m
			}()},
		},
		{
			name: "V-027 metric status forbidden",
			rule: "V-027",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.Metrics[0].Status = "degraded"
				return m
			}()},
		},
		{
			name: "V-030 provenance source type allowed",
			rule: "V-030",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.Models[0].Provenance.SourceType = "unknown"
				return m
			}()},
		},
		{
			name: "V-031 provenance confidence range",
			rule: "V-031",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.Models[0].Columns[0].Provenance.Confidence = 1.2
				return m
			}()},
		},
		{
			name: "V-032 user_defined forbidden in semantic",
			rule: "V-032",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.Models[0].Provenance.SourceType = "user_defined"
				return m
			}()},
		},
		{
			name: "V-041 matching smif versions",
			rule: "V-041",
			pass: ValidationDoc{
				Semantic: validSemanticModel(),
				Corrections: &CorrectionsFile{
					SMIFVersion: "0.1.0",
					Corrections: []Correction{validCorrection("model", "orders")},
				},
			},
			fail: ValidationDoc{
				Semantic: validSemanticModel(),
				Corrections: &CorrectionsFile{
					SMIFVersion: "0.1.1",
					Corrections: []Correction{validCorrection("model", "orders")},
				},
			},
		},
		{
			name: "V-042 target id resolves",
			rule: "V-042",
			pass: ValidationDoc{
				Semantic: validSemanticModel(),
				Corrections: &CorrectionsFile{
					SMIFVersion: "0.1.0",
					Corrections: []Correction{validCorrection("column", "orders.user_id")},
				},
			},
			fail: ValidationDoc{
				Semantic: validSemanticModel(),
				Corrections: &CorrectionsFile{
					SMIFVersion: "0.1.0",
					Corrections: []Correction{validCorrection("column", "orders.missing")},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mustPass, _ := Validate(tc.pass)
			if hasViolation(mustPass, tc.rule) {
				t.Fatalf("expected pass doc to not violate %s", tc.rule)
			}

			mustFail, _ := Validate(tc.fail)
			if !hasViolation(mustFail, tc.rule) {
				t.Fatalf("expected fail doc to violate %s", tc.rule)
			}
		})
	}
}

func TestValidateShouldRules(t *testing.T) {
	tests := []struct {
		name string
		rule string
		pass ValidationDoc
		fail ValidationDoc
	}{
		{
			name: "W-001 model description substantive",
			rule: "W-001",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.Models[0].Description = "short"
				return m
			}()},
		},
		{
			name: "W-002 non-identifier description substantive",
			rule: "W-002",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.Models[0].Columns[1].Description = "short"
				return m
			}()},
		},
		{
			name: "W-007 ddl fingerprint present",
			rule: "W-007",
			pass: ValidationDoc{Semantic: validSemanticModel()},
			fail: ValidationDoc{Semantic: func() *SemanticModel {
				m := validSemanticModel()
				m.Models[0].DDLFingerprint = ""
				return m
			}()},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, shouldPass := Validate(tc.pass)
			if hasViolation(shouldPass, tc.rule) {
				t.Fatalf("expected pass doc to not violate %s", tc.rule)
			}

			_, shouldFail := Validate(tc.fail)
			if !hasViolation(shouldFail, tc.rule) {
				t.Fatalf("expected fail doc to violate %s", tc.rule)
			}
		})
	}
}

func hasViolation(violations []Violation, id string) bool {
	for _, v := range violations {
		if v.RuleID == id {
			return true
		}
	}
	return false
}

func validCorrection(targetType, targetID string) Correction {
	return Correction{
		CorrectionID:   "corr_12345678",
		TargetType:     targetType,
		TargetID:       targetID,
		CorrectionType: "description_override",
		NewValue:       "value",
		Source:         "user_defined",
		Status:         "approved",
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
	}
}

func validSemanticModel() *SemanticModel {
	return &SemanticModel{
		SMIFVersion: "0.1.0",
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		ToolVersion: "0.1.0-dev",
		Source: Source{
			Type:            "postgres",
			HostFingerprint: "abc123",
			SchemaNames:     []string{"public"},
		},
		Domain: Domain{
			Name:        "commerce",
			Description: "Domain describing core ecommerce entities and metrics.",
			Provenance:  baseProvenance(),
		},
		Models: []Model{
			{
				ModelID:        "orders",
				Name:           "orders",
				Label:          "Orders",
				Grain:          "One row per order",
				Description:    "Orders model includes one row per customer order event.",
				PhysicalSource: PhysicalSource{Schema: "public", Table: "orders"},
				DDLFingerprint: "fp_orders",
				Columns: []Column{
					{Name: "id", DataType: "uuid", Role: "identifier", Label: "ID", Description: "Unique order identifier for each placed order.", Provenance: baseProvenance()},
					{Name: "user_id", DataType: "uuid", Role: "dimension", Label: "User", Description: "Identifier of the user who created the order.", Provenance: baseProvenance(), CardinalityCategory: "low", ExampleValues: []string{"u1"}},
				},
				Provenance: baseProvenance(),
			},
			{
				ModelID:        "users",
				Name:           "users",
				Label:          "Users",
				Grain:          "One row per user",
				Description:    "Users model includes one row per user account in the system.",
				PhysicalSource: PhysicalSource{Schema: "public", Table: "users"},
				DDLFingerprint: "fp_users",
				Columns: []Column{
					{Name: "id", DataType: "uuid", Role: "identifier", Label: "ID", Description: "Unique user identifier in the customer domain.", Provenance: baseProvenance()},
					{Name: "created_at", DataType: "timestamp", Role: "timestamp", Label: "Created At", Description: "Timestamp at which the user account was created.", Provenance: baseProvenance()},
				},
				Provenance: baseProvenance(),
			},
		},
		Relationships: []Relationship{
			{
				RelationshipID:   "rel_orders_users",
				FromModel:        "orders",
				FromColumn:       "user_id",
				ToModel:          "users",
				ToColumn:         "id",
				RelationshipType: "many_to_one",
				JoinCondition:    "orders.user_id = users.id",
				Preferred:        true,
				Provenance:       baseProvenance(),
			},
		},
		Metrics: []Metric{
			{
				MetricID:    "metric_total_orders",
				Name:        "total_orders",
				Label:       "Total Orders",
				Description: "Count of all orders created in the selected time window.",
				Expression:  "count(*)",
				Aggregation: "count",
				Additivity:  "additive",
				Provenance:  baseProvenance(),
			},
		},
	}
}

func baseProvenance() Provenance {
	return Provenance{SourceType: "llm_inferred", Confidence: 0.9, HumanReviewed: true}
}
