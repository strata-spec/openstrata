package smif

// Default returns the default confidence score for the given source type
// and difficulty, per the SMIF spec Section 7.4 table.
func Default(sourceType string, difficulty string) float64 {
	switch sourceType {
	case "schema_constraint":
		return 1.0
	case "ddl_comment":
		return 0.95
	case "strata_md":
		return 0.95
	case "catalog_import":
		return 0.85
	case "code_extracted":
		return 0.80
	case "log_inferred_high_support":
		return 0.75
	case "log_inferred_low_support":
		return 0.55
	case "llm_inferred":
		switch difficulty {
		case "self_evident":
			return 0.80
		case "context_dependent":
			return 0.65
		case "ambiguous":
			return 0.40
		case "domain_dependent":
			return 0.35
		default:
			return 0.50
		}
	default:
		return 0.50
	}
}

// DecayOnDrift applies the drift confidence decay rules from SMIF spec
// Section 7.3 to all fields in the model.
func DecayOnDrift(model *Model) {
	if model == nil {
		return
	}
	model.Provenance.Confidence *= 0.5
	model.Provenance.HumanReviewed = false
	for i := range model.Columns {
		model.Columns[i].Provenance.Confidence *= 0.5
		model.Columns[i].Provenance.HumanReviewed = false
	}
}
