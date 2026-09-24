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
	rawSchema *jsonschema.Schema
}

// NewSchemaValidator initializes validators from schema files in the given directory.
func NewSchemaValidator(contractsKafkaDir string) (*SchemaValidator, error) {
	rawSchemaPath := filepath.Join(contractsKafkaDir, "social.engagement.raw.schema.json")

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

	return &SchemaValidator{
		rawSchema: rawSchema,
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
