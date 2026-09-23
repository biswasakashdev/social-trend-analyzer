package validator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// SchemaValidator validates JSON byte arrays against compiled JSON schemas from /contracts/kafka.
type SchemaValidator struct {
	rawSchema        *jsonschema.Schema
	normalizedSchema *jsonschema.Schema
}

// NewSchemaValidator initializes validators from schema files in the given directory or file paths.
func NewSchemaValidator(contractsKafkaDir string) (*SchemaValidator, error) {
	rawSchemaPath := filepath.Join(contractsKafkaDir, "social.engagement.raw.schema.json")
	normSchemaPath := filepath.Join(contractsKafkaDir, "social.engagement.normalized.schema.json")

	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020

	rawBytes, err := os.ReadFile(rawSchemaPath)
	if err != nil {
		return nil, fmt.Errorf("reading raw schema from %s: %w", rawSchemaPath, err)
	}
	if err := compiler.AddResource("raw.json", bytes.NewReader(rawBytes)); err != nil {
		return nil, fmt.Errorf("adding raw schema resource: %w", err)
	}
	rawSchema, err := compiler.Compile("raw.json")
	if err != nil {
		return nil, fmt.Errorf("compiling raw schema: %w", err)
	}

	normCompiler := jsonschema.NewCompiler()
	normCompiler.Draft = jsonschema.Draft2020

	normBytes, err := os.ReadFile(normSchemaPath)
	if err != nil {
		return nil, fmt.Errorf("reading normalized schema from %s: %w", normSchemaPath, err)
	}
	if err := normCompiler.AddResource("normalized.json", bytes.NewReader(normBytes)); err != nil {
		return nil, fmt.Errorf("adding normalized schema resource: %w", err)
	}
	normalizedSchema, err := normCompiler.Compile("normalized.json")
	if err != nil {
		return nil, fmt.Errorf("compiling normalized schema: %w", err)
	}

	return &SchemaValidator{
		rawSchema:        rawSchema,
		normalizedSchema: normalizedSchema,
	}, nil
}

// ValidateRaw validates raw JSON bytes against the social.engagement.raw schema.
func (v *SchemaValidator) ValidateRaw(data []byte) error {
	var val any
	if err := json.Unmarshal(data, &val); err != nil {
		return fmt.Errorf("unmarshaling raw JSON for validation: %w", err)
	}

	if err := v.rawSchema.Validate(val); err != nil {
		return fmt.Errorf("validating raw event against schema: %w", err)
	}
	return nil
}

// ValidateNormalized validates normalized JSON bytes against the social.engagement.normalized schema.
func (v *SchemaValidator) ValidateNormalized(data []byte) error {
	var val any
	if err := json.Unmarshal(data, &val); err != nil {
		return fmt.Errorf("unmarshaling normalized JSON for validation: %w", err)
	}

	if err := v.normalizedSchema.Validate(val); err != nil {
		return fmt.Errorf("validating normalized event against schema: %w", err)
	}
	return nil
}
