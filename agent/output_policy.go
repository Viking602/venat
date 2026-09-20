package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/Viking602/venat/provider"
)

// ValidateJSON validates payload against the supported JSON Schema subset used
// by OutputPolicy.
func ValidateJSON(schemaRaw, payload json.RawMessage) error {
	if len(schemaRaw) == 0 {
		return nil
	}
	schema, err := parseOutputPolicySchema(schemaRaw)
	if err != nil {
		return err
	}
	_, err = validateStructuredOutputAgainstSchema(schema, string(payload))
	return err
}

// OutputPolicy controls structured-output validation and bounded repair.
// Native opts into provider JSON Schema output through ResponseFormat; Validate
// independently enables local validation. Existing policies remain local-only.
type OutputPolicy struct {
	Schema            json.RawMessage `json:"schema,omitempty"`
	Validate          bool            `json:"validate,omitempty"`
	Repair            bool            `json:"repair,omitempty"`
	MaxRepairAttempts int             `json:"maxRepairAttempts,omitempty"`
	Native            bool            `json:"native,omitempty"`
}

type outputPolicySchema struct {
	Type                 string                        `json:"type,omitempty"`
	Description          string                        `json:"description,omitempty"`
	Properties           map[string]outputPolicySchema `json:"properties,omitempty"`
	Required             []string                      `json:"required,omitempty"`
	Items                *outputPolicySchema           `json:"items,omitempty"`
	Enum                 []json.RawMessage             `json:"enum,omitempty"`
	AdditionalProperties *bool                         `json:"additionalProperties,omitempty"`
}

func prepareOutputPolicy(policy OutputPolicy) (*provider.ResponseFormat, error) {
	if err := ValidateOutputPolicy(policy); err != nil {
		return nil, err
	}
	if !policy.Native {
		return nil, nil
	}
	return &provider.ResponseFormat{
		Type: "json_schema", Name: "agent_output",
		RawSchema: append(json.RawMessage(nil), policy.Schema...),
	}, nil
}

// ValidateOutputPolicy checks static output configuration before an execution
// or dispatch can perform effects. It does not validate an output value.
func ValidateOutputPolicy(policy OutputPolicy) error {
	if policy.MaxRepairAttempts < 0 {
		return errors.New("negative repair attempt limit")
	}
	if policy.Native && len(policy.Schema) == 0 {
		return errors.New("native output requires a schema")
	}
	if (!policy.Validate && !policy.Native) || len(policy.Schema) == 0 {
		return nil
	}
	_, err := parseOutputPolicySchema(policy.Schema)
	return err
}

func parseOutputPolicySchema(schemaRaw json.RawMessage) (outputPolicySchema, error) {
	if err := validateContinuationJSONDocument(schemaRaw); err != nil {
		return outputPolicySchema{}, fmt.Errorf("output schema is invalid JSON: %w", err)
	}
	var schema *outputPolicySchema
	if err := decodeJSONUseNumber(schemaRaw, &schema); err != nil {
		return outputPolicySchema{}, fmt.Errorf("output schema is invalid JSON: %w", err)
	}
	if schema == nil {
		return outputPolicySchema{}, errors.New("output schema must be a JSON object")
	}
	if err := rejectUnsupportedOutputPolicySchemaKeywords(schemaRaw, "$"); err != nil {
		return outputPolicySchema{}, err
	}
	if err := validateOutputPolicySchema(*schema, "$"); err != nil {
		return outputPolicySchema{}, err
	}
	return *schema, nil
}

func rejectUnsupportedOutputPolicySchemaKeywords(schemaRaw json.RawMessage, path string) error {
	var fields *map[string]json.RawMessage
	if err := decodeJSONUseNumber(schemaRaw, &fields); err != nil {
		return fmt.Errorf("%s: schema must be a JSON object: %w", path, err)
	}
	if fields == nil {
		return fmt.Errorf("%s: schema must be a JSON object", path)
	}
	for keyword := range *fields {
		if !supportedOutputPolicySchemaKeyword(keyword) {
			return fmt.Errorf("%s: unsupported schema keyword %q", path, keyword)
		}
	}
	if propertiesRaw, ok := (*fields)["properties"]; ok {
		var properties map[string]json.RawMessage
		if err := decodeJSONUseNumber(propertiesRaw, &properties); err != nil {
			return fmt.Errorf("%s.properties: invalid properties: %w", path, err)
		}
		for name, propertyRaw := range properties {
			if err := rejectUnsupportedOutputPolicySchemaKeywords(propertyRaw, propertyPath(path, name)); err != nil {
				return err
			}
		}
	}
	if itemsRaw, ok := (*fields)["items"]; ok {
		if err := rejectUnsupportedOutputPolicySchemaKeywords(itemsRaw, path+".items"); err != nil {
			return err
		}
	}
	return nil
}

func supportedOutputPolicySchemaKeyword(keyword string) bool {
	switch keyword {
	case "type", "properties", "required", "items", "enum", "additionalProperties", "description":
		return true
	default:
		return false
	}
}

func validateOutputPolicySchema(schema outputPolicySchema, path string) error {
	if schema.Type != "" && !supportedJSONSchemaType(schema.Type) {
		return fmt.Errorf("%s: unsupported schema type %q", path, schema.Type)
	}
	for name, propertySchema := range schema.Properties {
		if err := validateOutputPolicySchema(propertySchema, propertyPath(path, name)); err != nil {
			return err
		}
	}
	if schema.Items != nil {
		if err := validateOutputPolicySchema(*schema.Items, path+".items"); err != nil {
			return err
		}
	}
	return nil
}

func supportedJSONSchemaType(schemaType string) bool {
	switch schemaType {
	case "object", "array", "string", "number", "integer", "boolean":
		return true
	default:
		return false
	}
}

func validateStructuredOutputAgainstSchema(schema outputPolicySchema, text string) (json.RawMessage, error) {
	var value any
	if err := decodeJSONUseNumber([]byte(text), &value); err != nil {
		return nil, fmt.Errorf("agent terminal output is not valid JSON: %w", err)
	}
	if err := validateJSONValue(value, schema, "$"); err != nil {
		return nil, err
	}
	return json.RawMessage(text), nil
}

func decodeJSONUseNumber(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func validateJSONValue(value any, schema outputPolicySchema, path string) error {
	if schema.Type != "" {
		if err := validateJSONType(value, schema.Type, path); err != nil {
			return err
		}
	}
	if err := validateJSONEnum(value, schema, path); err != nil {
		return err
	}
	if err := validateJSONObject(value, schema, path); err != nil {
		return err
	}
	return validateJSONArray(value, schema, path)
}

func validateJSONType(value any, schemaType string, path string) error {
	switch schemaType {
	case "object":
		if _, ok := value.(map[string]any); ok {
			return nil
		}
	case "array":
		if _, ok := value.([]any); ok {
			return nil
		}
	case "string":
		if _, ok := value.(string); ok {
			return nil
		}
	case "number":
		if number, ok := value.(json.Number); ok && validJSONNumber(number) {
			return nil
		}
	case "integer":
		if number, ok := value.(json.Number); ok && integerJSONNumber(number) {
			return nil
		}
	case "boolean":
		if _, ok := value.(bool); ok {
			return nil
		}
	default:
		return fmt.Errorf("%s: unsupported schema type %q", path, schemaType)
	}
	return fmt.Errorf("%s: expected %s", path, schemaType)
}

func validateJSONEnum(value any, schema outputPolicySchema, path string) error {
	if len(schema.Enum) == 0 {
		return nil
	}
	matches, err := valueMatchesEnum(value, schema.Enum)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if !matches {
		return fmt.Errorf("%s: value is not in enum", path)
	}
	return nil
}

func validateJSONObject(value any, schema outputPolicySchema, path string) error {
	if !schemaRequiresObject(schema) {
		return nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("%s: expected object", path)
	}
	if err := validateRequiredProperties(object, schema, path); err != nil {
		return err
	}
	if err := validateDefinedProperties(object, schema, path); err != nil {
		return err
	}
	return validateAdditionalProperties(object, schema, path)
}

func schemaRequiresObject(schema outputPolicySchema) bool {
	return schema.Type == "object" || len(schema.Properties) > 0 || len(schema.Required) > 0 || schema.AdditionalProperties != nil
}

func validateRequiredProperties(object map[string]any, schema outputPolicySchema, path string) error {
	for _, name := range schema.Required {
		if _, ok := object[name]; !ok {
			return fmt.Errorf("%s: missing required property %q", path, name)
		}
	}
	return nil
}

func validateDefinedProperties(object map[string]any, schema outputPolicySchema, path string) error {
	for name, propertySchema := range schema.Properties {
		propertyValue, ok := object[name]
		if !ok {
			continue
		}
		if err := validateJSONValue(propertyValue, propertySchema, propertyPath(path, name)); err != nil {
			return err
		}
	}
	return nil
}

func validateAdditionalProperties(object map[string]any, schema outputPolicySchema, path string) error {
	if schema.AdditionalProperties == nil || *schema.AdditionalProperties {
		return nil
	}
	for name := range object {
		if _, ok := schema.Properties[name]; !ok {
			return fmt.Errorf("%s: additional property %q is not allowed", path, name)
		}
	}
	return nil
}

func validateJSONArray(value any, schema outputPolicySchema, path string) error {
	if schema.Type != "array" && schema.Items == nil {
		return nil
	}
	array, ok := value.([]any)
	if !ok {
		return fmt.Errorf("%s: expected array", path)
	}
	if schema.Items == nil {
		return nil
	}
	for index, item := range array {
		if err := validateJSONValue(item, *schema.Items, fmt.Sprintf("%s[%d]", path, index)); err != nil {
			return err
		}
	}
	return nil
}

func validJSONNumber(number json.Number) bool {
	_, ok := new(big.Rat).SetString(number.String())
	return ok
}

func integerJSONNumber(number json.Number) bool {
	rat, ok := new(big.Rat).SetString(number.String())
	return ok && rat.IsInt()
}

func valueMatchesEnum(value any, enum []json.RawMessage) (bool, error) {
	for _, raw := range enum {
		var expected any
		if err := decodeJSONUseNumber(raw, &expected); err != nil {
			return false, fmt.Errorf("invalid enum value: %w", err)
		}
		if equalJSONValue(value, expected) {
			return true, nil
		}
	}
	return false, nil
}

func equalJSONValue(left any, right any) bool {
	switch leftValue := left.(type) {
	case nil:
		return right == nil
	case string:
		rightValue, ok := right.(string)
		return ok && leftValue == rightValue
	case bool:
		rightValue, ok := right.(bool)
		return ok && leftValue == rightValue
	case json.Number:
		rightValue, ok := right.(json.Number)
		return ok && equalJSONNumber(leftValue, rightValue)
	case []any:
		rightValue, ok := right.([]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for index := range leftValue {
			if !equalJSONValue(leftValue[index], rightValue[index]) {
				return false
			}
		}
		return true
	case map[string]any:
		rightValue, ok := right.(map[string]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for key, nestedLeft := range leftValue {
			nestedRight, ok := rightValue[key]
			if !ok || !equalJSONValue(nestedLeft, nestedRight) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func equalJSONNumber(left json.Number, right json.Number) bool {
	leftRat, ok := new(big.Rat).SetString(left.String())
	if !ok {
		return false
	}
	rightRat, ok := new(big.Rat).SetString(right.String())
	return ok && leftRat.Cmp(rightRat) == 0
}

func propertyPath(path string, property string) string {
	if path == "$" {
		return "$." + property
	}
	return path + "." + property
}
