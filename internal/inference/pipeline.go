package inference

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	coarsepkg "github.com/strata-spec/openstrata/internal/inference/coarse"
	finepkg "github.com/strata-spec/openstrata/internal/inference/fine"
	joinspkg "github.com/strata-spec/openstrata/internal/inference/joins"
	"github.com/strata-spec/openstrata/internal/inference/llm"
	"github.com/strata-spec/openstrata/internal/postgres"
	"github.com/strata-spec/openstrata/internal/smif"
	"gopkg.in/yaml.v3"
)

// Config holds the parameters for an inference pipeline run.
type Config struct {
	DSN             string
	Schema          string
	EnableLogMining bool
	StrataMDPath    string
	LLM             llm.LLMClient
}

// Init runs the full inference pipeline (Stages 1–9) and writes
// semantic.yaml, semantic.json, and corrections.yaml.
func Init(ctx context.Context, cfg Config) error {
	strataMD, strataMDFound, err := Load(cfg.StrataMDPath)
	if err != nil {
		return fmt.Errorf("init: stage 1 load strata.md: %w", err)
	}

	hostFingerprint, err := postgres.Fingerprint(cfg.DSN)
	if err != nil {
		return fmt.Errorf("init: host fingerprint: %w", err)
	}

	pool, err := postgres.Connect(ctx, cfg.DSN)
	if err != nil {
		return fmt.Errorf("init: stage 2 connect: %w", err)
	}
	defer pool.Close()

	tables, err := postgres.Introspect(ctx, pool, cfg.Schema)
	if err != nil {
		return fmt.Errorf("init: stage 2 introspect: %w", err)
	}

	profiles, err := postgres.Profile(ctx, pool, tables)
	if err != nil {
		return fmt.Errorf("init: stage 3 profile: %w", err)
	}

	var usageProfiles []postgres.UsageProfile
	if cfg.EnableLogMining {
		usageProfiles, err = postgres.Mine(ctx, pool)
		if err != nil {
			return fmt.Errorf("init: stage 4 log mining: %w", err)
		}
	}

	domainResult, err := RunDomainPass(ctx, cfg.LLM, tables, strataMD)
	if err != nil {
		return fmt.Errorf("init: stage 5 domain pass: %w", err)
	}

	tableResults, err := RunTablePass(ctx, cfg.LLM, tables, domainResult, strataMD)
	if err != nil {
		return fmt.Errorf("init: stage 5 table pass: %w", err)
	}

	fineResults, err := RunFinePass(ctx, cfg.LLM, tables, profiles, tableResults, domainResult, strataMD)
	if err != nil {
		return fmt.Errorf("init: stage 6 fine pass: %w", err)
	}

	inferredRelationships, err := InferJoins(tables, usageProfiles, strataMD)
	if err != nil {
		return fmt.Errorf("init: stage 7 infer joins: %w", err)
	}
	grainConfirmations := ConfirmGrains(tables, toCoarseTableResults(tableResults))

	model, err := assembleModel(
		cfg,
		toCoarseDomainResult(domainResult),
		tables,
		profiles,
		toCoarseTableResults(tableResults),
		toFineResults(fineResults),
		toJoinRelationships(inferredRelationships),
		toJoinGrainConfirmations(grainConfirmations),
		usageProfiles,
		strataMDFound,
		hostFingerprint,
	)
	if err != nil {
		return fmt.Errorf("init: stage 8 assemble model: %w", err)
	}

	musts, shoulds := smif.Validate(smif.ValidationDoc{Semantic: model})
	if len(musts) > 0 {
		lines := make([]string, 0, len(musts))
		for _, v := range musts {
			msg := fmt.Sprintf("V-%s: %s - %s", strings.TrimPrefix(v.RuleID, "V-"), v.Path, v.Message)
			fmt.Fprintln(os.Stderr, msg)
			lines = append(lines, msg)
		}
		return fmt.Errorf("init: validation failed:\n%s", strings.Join(lines, "\n"))
	}

	for _, v := range shoulds {
		fmt.Fprintf(os.Stderr, "W-%s: %s - %s\n", strings.TrimPrefix(v.RuleID, "W-"), v.Path, v.Message)
	}

	if err := writeOutputs(model, "."); err != nil {
		return fmt.Errorf("init: stage 9 write outputs: %w", err)
	}

	printSummary(model, musts, shoulds)
	_ = strataMDFound
	return nil
}

// Refresh re-runs inference for changed models only, merging with
// existing corrections. Implements the algorithm in DESIGN.md Section 9.
func Refresh(ctx context.Context, cfg Config) error {
	semanticPath := filepath.Clean("semantic.yaml")
	correctionsPath := filepath.Clean("corrections.yaml")

	existing, err := smif.ReadYAML(semanticPath)
	if err != nil {
		return fmt.Errorf("refresh: read semantic.yaml: %w", err)
	}

	corrections, err := readCorrectionsFile(correctionsPath)
	if err != nil {
		return fmt.Errorf("refresh: read corrections.yaml: %w", err)
	}

	strataMD, strataMDFound, err := Load(cfg.StrataMDPath)
	if err != nil {
		return fmt.Errorf("refresh: stage 1 load strata.md: %w", err)
	}

	hostFingerprint, err := postgres.Fingerprint(cfg.DSN)
	if err != nil {
		return fmt.Errorf("refresh: host fingerprint: %w", err)
	}

	pool, err := postgres.Connect(ctx, cfg.DSN)
	if err != nil {
		return fmt.Errorf("refresh: stage 2 connect: %w", err)
	}
	defer pool.Close()

	liveTables, err := postgres.Introspect(ctx, pool, cfg.Schema)
	if err != nil {
		return fmt.Errorf("refresh: stage 2 introspect: %w", err)
	}

	profiles, err := postgres.Profile(ctx, pool, liveTables)
	if err != nil {
		return fmt.Errorf("refresh: stage 3 profile: %w", err)
	}

	var usageProfiles []postgres.UsageProfile
	if cfg.EnableLogMining {
		usageProfiles, err = postgres.Mine(ctx, pool)
		if err != nil {
			return fmt.Errorf("refresh: stage 4 log mining: %w", err)
		}
	}

	existingByName := make(map[string]smif.Model, len(existing.Models))
	for _, m := range existing.Models {
		existingByName[strings.ToLower(strings.TrimSpace(m.Name))] = m
	}

	liveByName := make(map[string]postgres.TableInfo, len(liveTables))
	for _, t := range liveTables {
		liveByName[strings.ToLower(strings.TrimSpace(t.Name))] = t
	}

	changedOrNew := make([]postgres.TableInfo, 0)
	for _, t := range liveTables {
		name := strings.ToLower(strings.TrimSpace(t.Name))
		liveFP := smif.Compute(t)
		existingModel, ok := existingByName[name]
		if !ok {
			changedOrNew = append(changedOrNew, t)
			fmt.Fprintf(os.Stderr, "warning: model %s is new; inferring\n", t.Name)
			continue
		}
		if existingModel.DDLFingerprint != liveFP {
			changedOrNew = append(changedOrNew, t)
			fmt.Fprintf(os.Stderr, "warning: model %s schema has changed; re-inferring\n", t.Name)
		}
	}

	for _, m := range existing.Models {
		name := strings.ToLower(strings.TrimSpace(m.Name))
		if _, ok := liveByName[name]; !ok {
			fmt.Fprintf(os.Stderr, "warning: model %s is not present in live schema\n", m.Name)
		}
	}

	var domainResult *DomainResult
	var tableResults []TableResult
	var fineResults []FinePassResult
	if len(changedOrNew) > 0 {
		domainResult, err = RunDomainPass(ctx, cfg.LLM, liveTables, strataMD)
		if err != nil {
			return fmt.Errorf("refresh: stage 5 domain pass: %w", err)
		}

		tableResults, err = RunTablePass(ctx, cfg.LLM, changedOrNew, domainResult, strataMD)
		if err != nil {
			return fmt.Errorf("refresh: stage 5 table pass: %w", err)
		}

		fineResults, err = RunFinePass(ctx, cfg.LLM, changedOrNew, profiles, tableResults, domainResult, strataMD)
		if err != nil {
			return fmt.Errorf("refresh: stage 6 fine pass: %w", err)
		}
	}

	relationships, err := InferJoins(liveTables, usageProfiles, strataMD)
	if err != nil {
		return fmt.Errorf("refresh: stage 7 infer joins: %w", err)
	}

	grainConfirmations := ConfirmGrains(liveTables, toCoarseTableResults(tableResults))

	draft, err := assembleModel(
		cfg,
		toCoarseDomainResult(domainResult),
		liveTables,
		profiles,
		toCoarseTableResults(tableResults),
		toFineResults(fineResults),
		toJoinRelationships(relationships),
		toJoinGrainConfirmations(grainConfirmations),
		usageProfiles,
		strataMDFound,
		hostFingerprint,
	)
	if err != nil {
		return fmt.Errorf("refresh: stage 8 assemble model: %w", err)
	}

	approvedLocked := approvedUserDefinedTargetIDs(corrections)
	for i := range draft.Models {
		m := draft.Models[i]
		name := strings.ToLower(strings.TrimSpace(m.Name))
		oldModel, ok := existingByName[name]
		if !ok {
			continue
		}

		isChanged := oldModel.DDLFingerprint != m.DDLFingerprint
		if !isChanged {
			draft.Models[i] = oldModel
			continue
		}

		draft.Models[i] = preserveLockedColumns(oldModel, m, approvedLocked)
	}

	musts, shoulds := smif.Validate(smif.ValidationDoc{Semantic: draft, Corrections: corrections})
	if len(musts) > 0 {
		lines := make([]string, 0, len(musts))
		for _, v := range musts {
			msg := fmt.Sprintf("V-%s: %s - %s", strings.TrimPrefix(v.RuleID, "V-"), v.Path, v.Message)
			fmt.Fprintln(os.Stderr, msg)
			lines = append(lines, msg)
		}
		return fmt.Errorf("refresh: validation failed:\n%s", strings.Join(lines, "\n"))
	}

	for _, v := range shoulds {
		fmt.Fprintf(os.Stderr, "W-%s: %s - %s\n", strings.TrimPrefix(v.RuleID, "W-"), v.Path, v.Message)
	}

	if err := smif.WriteYAML(semanticPath, draft); err != nil {
		return fmt.Errorf("refresh: write semantic.yaml: %w", err)
	}
	if err := smif.WriteJSON("semantic.json", draft); err != nil {
		return fmt.Errorf("refresh: write semantic.json: %w", err)
	}

	printSummary(draft, musts, shoulds)
	return nil
}

func assembleModel(
	cfg Config,
	domain *coarsepkg.DomainResult,
	tables []postgres.TableInfo,
	profiles map[string]postgres.ColumnProfile,
	tableResults []coarsepkg.TableResult,
	fineResults []finepkg.FinePassResult,
	relationships []joinspkg.InferredRelationship,
	grainConfirmations []joinspkg.GrainConfirmation,
	usageProfiles []postgres.UsageProfile,
	strataMDFound bool,
	hostFingerprint string,
) (*smif.SemanticModel, error) {
	_ = strataMDFound

	tableByName := make(map[string]postgres.TableInfo, len(tables))
	for _, t := range tables {
		tableByName[strings.ToLower(t.Name)] = t
	}

	tableResultByName := make(map[string]coarsepkg.TableResult, len(tableResults))
	for _, tr := range tableResults {
		tableResultByName[strings.ToLower(tr.TableName)] = tr
	}

	grainByTable := make(map[string]joinspkg.GrainConfirmation, len(grainConfirmations))
	for _, gc := range grainConfirmations {
		grainByTable[strings.ToLower(gc.TableName)] = gc
	}

	fineByTableCol := make(map[string]finepkg.ColumnResult)
	for _, fr := range fineResults {
		for _, c := range fr.Columns {
			k := strings.ToLower(fr.TableName + "." + c.ColumnName)
			fineByTableCol[k] = c
		}
	}

	usageByTableCol := make(map[string]postgres.UsageProfile, len(usageProfiles))
	for _, up := range usageProfiles {
		usageByTableCol[strings.ToLower(up.TableName+"."+up.ColumnName)] = up
	}

	models := make([]smif.Model, 0, len(tables))
	for _, t := range tables {
		tr := tableResultByName[strings.ToLower(t.Name)]
		gc, hasGrain := grainByTable[strings.ToLower(t.Name)]
		_ = hasGrain

		model := smif.Model{
			ModelID:        normalizeName(t.Name),
			Name:           t.Name,
			Label:          titleCaseName(t.Name),
			Description:    tr.Description,
			PhysicalSource: smif.PhysicalSource{Schema: cfg.Schema, Table: t.Name},
			PrimaryKey:     append([]string(nil), t.PrimaryKey...),
			DDLFingerprint: smif.Compute(t),
			Columns:        make([]smif.Column, 0, len(t.Columns)),
			Provenance: smif.Provenance{
				SourceType:    "schema_constraint",
				Confidence:    0.5,
				HumanReviewed: false,
			},
			XProperties: map[string]any{},
		}
		if strings.TrimSpace(model.Description) != "" {
			model.Provenance.Confidence = 1.0
		}
		if hasGrain {
			model.XProperties["grain"] = gc.GrainStatement
		}

		for _, col := range t.Columns {
			colKey := strings.ToLower(t.Name + "." + col.Name)
			profile := profiles[t.Name+"."+col.Name]
			fineCol, hasFine := fineByTableCol[colKey]

			role := "dimension"
			label := titleCaseName(col.Name)
			desc := ""
			difficulty := ""
			needsReview := false
			if hasFine {
				if strings.TrimSpace(fineCol.Role) != "" {
					role = fineCol.Role
				}
				if strings.TrimSpace(fineCol.Label) != "" {
					label = fineCol.Label
				}
				desc = fineCol.Description
				difficulty = fineCol.Difficulty
				needsReview = fineCol.NeedsReview
			}

			sourceType := "schema_constraint"
			if hasFine {
				sourceType = "llm_inferred"
			}
			if strings.TrimSpace(col.Comment) != "" {
				sourceType = "ddl_comment"
			}

			smifCol := smif.Column{
				Name:                col.Name,
				DataType:            col.DataType,
				Role:                role,
				Label:               label,
				Description:         desc,
				Nullable:            col.IsNullable,
				CardinalityCategory: profile.CardinalityCategory,
				ExampleValues:       append([]string(nil), profile.ExampleValues...),
				Difficulty:          difficulty,
				Provenance: smif.Provenance{
					SourceType:    sourceType,
					Confidence:    smif.Default(sourceType, difficulty),
					HumanReviewed: false,
				},
				XProperties: map[string]any{"needs_review": needsReview},
			}

			if strings.TrimSpace(smifCol.CardinalityCategory) == "" {
				smifCol.CardinalityCategory = "unknown"
			}

			if up, ok := usageByTableCol[colKey]; ok {
				smifCol.UsageProfile = &smif.UsageProfile{
					SelectFrequency:  float64(up.SelectCount),
					WhereFrequency:   float64(up.WhereCount),
					GroupByFrequency: float64(up.GroupByCount),
					JoinFrequency:    float64(up.JoinCount),
				}
			}

			model.Columns = append(model.Columns, smifCol)
		}

		models = append(models, model)
	}

	rels := make([]smif.Relationship, 0, len(relationships))
	for _, r := range relationships {
		rels = append(rels, smif.Relationship{
			RelationshipID:   r.RelationshipID,
			FromModel:        r.FromModel,
			FromColumn:       r.FromColumn,
			ToModel:          r.ToModel,
			ToColumn:         r.ToColumn,
			RelationshipType: r.RelationshipType,
			JoinCondition:    r.JoinCondition,
			AlwaysValid:      true,
			Preferred:        r.Preferred,
			Provenance: smif.Provenance{
				SourceType:    r.SourceType,
				Confidence:    r.Confidence,
				HumanReviewed: false,
			},
		})
	}

	domainOut := smif.Domain{
		Provenance: smif.Provenance{
			SourceType:    "llm_inferred",
			Confidence:    0.8,
			HumanReviewed: false,
		},
	}
	if domain != nil {
		domainOut.Name = domain.Name
		domainOut.Description = domain.Description
		domainOut.KeyConcepts = append([]string(nil), domain.KeyConcepts...)
		domainOut.KnownGotchas = append([]string(nil), domain.KnownGotchas...)
		if strings.TrimSpace(domain.TemporalModel) != "" {
			domainOut.Temporal = &smif.TemporalModel{Note: domain.TemporalModel}
		}
	}

	model := &smif.SemanticModel{
		SMIFVersion:   "0.1.0",
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		ToolVersion:   "strata/0.1.0-dev",
		Source:        smif.Source{Type: "postgres", HostFingerprint: hostFingerprint, SchemaNames: []string{cfg.Schema}},
		Domain:        domainOut,
		Models:        models,
		Relationships: rels,
		Metrics:       []smif.Metric{},
		Concepts:      []smif.Concept{},
	}

	return model, nil
}

func writeOutputs(model *smif.SemanticModel, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	yamlPath := filepath.Join(dir, "semantic.yaml")
	if err := smif.WriteYAML(yamlPath, model); err != nil {
		return fmt.Errorf("write semantic.yaml: %w", err)
	}

	jsonPath := filepath.Join(dir, "semantic.json")
	if err := smif.WriteJSON(jsonPath, model); err != nil {
		return fmt.Errorf("write semantic.json: %w", err)
	}

	correctionsPath := filepath.Join(dir, "corrections.yaml")
	const emptyCorrections = "smif_version: \"0.1.0\"\ncorrections:\n"
	if err := os.WriteFile(correctionsPath, []byte(emptyCorrections), 0o644); err != nil {
		return fmt.Errorf("write corrections.yaml: %w", err)
	}

	return nil
}

func printSummary(model *smif.SemanticModel, musts, shoulds []smif.Violation) {
	_ = musts

	modelCount := len(model.Models)
	columnCount := 0
	flagged := 0
	for _, m := range model.Models {
		columnCount += len(m.Columns)
		for _, c := range m.Columns {
			if c.XProperties == nil {
				continue
			}
			if v, ok := c.XProperties["needs_review"].(bool); ok && v {
				flagged++
			}
		}
	}

	relCount := len(model.Relationships)
	schemaCount := 0
	logCount := 0
	for _, r := range model.Relationships {
		switch r.Provenance.SourceType {
		case "schema_constraint":
			schemaCount++
		case "log_inferred":
			logCount++
		}
	}

	fmt.Println("✓ Strata init complete")
	fmt.Printf("  Models:        %d\n", modelCount)
	fmt.Printf("  Columns:       %d\n", columnCount)
	fmt.Printf("  Relationships: %d (%d schema, %d log-inferred)\n", relCount, schemaCount, logCount)
	fmt.Println("  Metrics:       0")
	fmt.Printf("  Flagged:       %d columns need human review\n", flagged)

	if len(shoulds) > 0 {
		fmt.Printf("\n  Warnings (%d):\n", len(shoulds))
		for _, v := range shoulds {
			fmt.Printf("  W-%s: %s - %s\n", strings.TrimPrefix(v.RuleID, "W-"), v.Path, v.Message)
		}
	}

	fmt.Println("\n  Run 'strata validate' to check for spec violations.")
	fmt.Println("  Run 'strata serve' to start the MCP server.")
}

func toCoarseDomainResult(domain *DomainResult) *coarsepkg.DomainResult {
	if domain == nil {
		return nil
	}
	return &coarsepkg.DomainResult{
		Name:          domain.Name,
		Description:   domain.Description,
		KeyConcepts:   append([]string(nil), domain.KeyConcepts...),
		TemporalModel: domain.TemporalModel,
		KnownGotchas:  append([]string(nil), domain.KnownGotchas...),
	}
}

func toCoarseTableResults(in []TableResult) []coarsepkg.TableResult {
	out := make([]coarsepkg.TableResult, 0, len(in))
	for _, v := range in {
		out = append(out, coarsepkg.TableResult{TableName: v.TableName, Description: v.Description, Grain: v.Grain})
	}
	return out
}

func toFineResults(in []FinePassResult) []finepkg.FinePassResult {
	out := make([]finepkg.FinePassResult, 0, len(in))
	for _, v := range in {
		cols := make([]finepkg.ColumnResult, 0, len(v.Columns))
		for _, c := range v.Columns {
			cols = append(cols, finepkg.ColumnResult{
				TableName:   c.TableName,
				ColumnName:  c.ColumnName,
				Role:        c.Role,
				Label:       c.Label,
				Description: c.Description,
				Difficulty:  c.Difficulty,
				NeedsReview: c.NeedsReview,
			})
		}
		out = append(out, finepkg.FinePassResult{TableName: v.TableName, Columns: cols})
	}
	return out
}

func toJoinRelationships(in []InferredRelationship) []joinspkg.InferredRelationship {
	out := make([]joinspkg.InferredRelationship, 0, len(in))
	for _, v := range in {
		out = append(out, joinspkg.InferredRelationship{
			RelationshipID:   v.RelationshipID,
			FromModel:        v.FromModel,
			FromColumn:       v.FromColumn,
			ToModel:          v.ToModel,
			ToColumn:         v.ToColumn,
			RelationshipType: v.RelationshipType,
			JoinCondition:    v.JoinCondition,
			SourceType:       v.SourceType,
			Confidence:       v.Confidence,
			Preferred:        v.Preferred,
		})
	}
	return out
}

func toJoinGrainConfirmations(in []GrainConfirmation) []joinspkg.GrainConfirmation {
	out := make([]joinspkg.GrainConfirmation, 0, len(in))
	for _, v := range in {
		out = append(out, joinspkg.GrainConfirmation{
			TableName:      v.TableName,
			GrainStatement: v.GrainStatement,
			PKColumns:      append([]string(nil), v.PKColumns...),
			Confirmed:      v.Confirmed,
			Note:           v.Note,
		})
	}
	return out
}

func normalizeName(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.ReplaceAll(s, " ", "_")
	return s
}

func titleCaseName(s string) string {
	parts := strings.Fields(strings.ReplaceAll(s, "_", " "))
	for i := range parts {
		if len(parts[i]) == 0 {
			continue
		}
		parts[i] = strings.ToUpper(parts[i][:1]) + strings.ToLower(parts[i][1:])
	}
	return strings.Join(parts, " ")
}

func readCorrectionsFile(path string) (*smif.CorrectionsFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &smif.CorrectionsFile{SMIFVersion: "0.1.0", Corrections: []smif.Correction{}}, nil
		}
		return nil, err
	}

	var c smif.CorrectionsFile
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if c.Corrections == nil {
		c.Corrections = []smif.Correction{}
	}
	if strings.TrimSpace(c.SMIFVersion) == "" {
		c.SMIFVersion = "0.1.0"
	}
	return &c, nil
}

func approvedUserDefinedTargetIDs(corrections *smif.CorrectionsFile) map[string]struct{} {
	locked := make(map[string]struct{})
	if corrections == nil {
		return locked
	}
	for _, c := range corrections.Corrections {
		if strings.EqualFold(c.Source, "user_defined") && strings.EqualFold(c.Status, "approved") {
			locked[strings.ToLower(strings.TrimSpace(c.TargetID))] = struct{}{}
		}
	}
	return locked
}

func preserveLockedColumns(oldModel, newModel smif.Model, locked map[string]struct{}) smif.Model {
	oldByColumn := make(map[string]smif.Column, len(oldModel.Columns))
	for _, c := range oldModel.Columns {
		oldByColumn[strings.ToLower(c.Name)] = c
	}

	out := newModel
	out.Columns = make([]smif.Column, 0, len(newModel.Columns))
	for _, c := range newModel.Columns {
		target1 := strings.ToLower(newModel.ModelID + "." + c.Name)
		target2 := strings.ToLower(newModel.Name + "." + c.Name)
		if _, ok := locked[target1]; ok {
			if old, found := oldByColumn[strings.ToLower(c.Name)]; found {
				out.Columns = append(out.Columns, old)
				continue
			}
		}
		if _, ok := locked[target2]; ok {
			if old, found := oldByColumn[strings.ToLower(c.Name)]; found {
				out.Columns = append(out.Columns, old)
				continue
			}
		}
		out.Columns = append(out.Columns, c)
	}

	return out
}

func sortedModelNames(models []smif.Model) []string {
	names := make([]string, 0, len(models))
	for _, m := range models {
		names = append(names, m.Name)
	}
	sort.Strings(names)
	return names
}
