package message

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// MaxToolNameBytes bounds identifiers received from model providers and tools.
const MaxToolNameBytes = 256

// JSONLimits bounds work before a JSON document is materialized. All limits
// must be positive. Collection bounds both array elements and object members.
type JSONLimits struct {
	Bytes      int
	Depth      int
	Values     int
	Collection int
}

// CheckJSON validates one document, including duplicate keys, within limits.
// It retains only bounded object-key sets rather than the decoded value graph.
func CheckJSON(data []byte, limits JSONLimits) error {
	if limits.Bytes <= 0 || limits.Depth <= 0 || limits.Values <= 0 || limits.Collection <= 0 {
		return errors.New("JSON limits must be positive")
	}
	if len(data) > limits.Bytes {
		return fmt.Errorf("JSON exceeds %d bytes", limits.Bytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	budget := jsonBudget{limits: limits}
	if err := budget.value(decoder, 1); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("JSON contains trailing data")
	}
	return nil
}

type jsonBudget struct {
	limits JSONLimits
	values int
}

func (budget *jsonBudget) value(decoder *json.Decoder, depth int) error {
	budget.values++
	if depth > budget.limits.Depth || budget.values > budget.limits.Values {
		return errors.New("JSON exceeds depth or value limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	if delimiter != '{' && delimiter != '[' {
		return errors.New("JSON has unexpected delimiter")
	}
	return budget.collection(decoder, delimiter, depth)
}

func (budget *jsonBudget) collection(decoder *json.Decoder, delimiter json.Delim, depth int) error {
	var keys map[string]struct{}
	if delimiter == '{' {
		keys = make(map[string]struct{})
	}
	count := 0
	for decoder.More() {
		count++
		if count > budget.limits.Collection {
			return errors.New("JSON exceeds collection limit")
		}
		if keys != nil {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok {
				return errors.New("JSON object key is not a string")
			}
			if _, duplicate := keys[key]; duplicate {
				return errors.New("JSON contains a duplicate object field")
			}
			keys[key] = struct{}{}
		}
		if err := budget.value(decoder, depth+1); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	want := json.Delim(']')
	if delimiter == '{' {
		want = '}'
	}
	if closing != want {
		return errors.New("JSON has invalid closing delimiter")
	}
	return nil
}
