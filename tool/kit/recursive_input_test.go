package kit

import (
	"strings"
	"testing"
)

type recursiveMap map[string]recursiveMap
type recursiveSlice []recursiveSlice
type recursivePointer *recursivePointer

func TestTool_RejectsRecursiveInputsAtConstruction(t *testing.T) {
	type node struct {
		Name string `json:"name"`
		Next *node  `json:"next,omitempty"`
	}
	for _, fn := range []any{
		func(node) (string, error) { return "", nil },
		func(recursiveMap) (string, error) { return "", nil },
		func(recursiveSlice) (string, error) { return "", nil },
		func(recursivePointer) (string, error) { return "", nil },
	} {
		if _, err := Tool("recursive", fn); err == nil || !strings.Contains(err.Error(), "recursive") {
			t.Fatalf("Tool() recursive error = %v", err)
		}
	}
	if _, err := Tool("map", func(map[string]any) (string, error) { return "", nil }); err != nil {
		t.Fatalf("non-recursive map rejected: %v", err)
	}
}
