package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestScanFile_RejectsLooseAnySpellingsAndBorrowedTags(t *testing.T) {
	source := `package sample

type hiddenAny map[string]any

type embeddedAny struct {
	Exposed any
}

type Exported struct {
	// godoc-allow-any
	Tagged any
	// godoc-allow-any: provider wire extension
	Reasoned map[string]any
	Borrowed any // godoc-allow-any
	Untagged any
	Empty interface{}
	Nested map[string]interface{}
	Alias hiddenAny
	embeddedAny
	unexported any
}

func Values() []interface{} { return nil }

//venat:allow-public-any
func Allowed() []any { return nil }
`
	path := filepath.Join(t.TempDir(), "sample.go")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	violations, err := scanFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := make([]int, 0, len(violations))
	for _, current := range violations {
		lines = append(lines, current.line)
	}
	slices.Sort(lines)
	// Borrowed(8) carries only a trailing comment; Untagged(9) must not inherit
	// it; interface{} spellings(10, 11, 15) are the same loose any.
	if want := []int{14, 15, 16, 17, 18, 19, 23}; !slices.Equal(lines, want) {
		t.Fatalf("violation lines = %v, want %v", lines, want)
	}
}
