package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var requiredScopes = []string{
	"message",
	"provider",
	"tool",
	"skill",
	"agent",
}

type violation struct {
	path    string
	line    int
	message string
}

func main() {
	var violations []violation
	for _, scope := range requiredScopes {
		current, err := scanScope(scope)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAIL [public-any]: %v\n", err)
			os.Exit(1)
		}
		violations = append(violations, current...)
	}
	sort.Slice(violations, func(left, right int) bool {
		if violations[left].path != violations[right].path {
			return violations[left].path < violations[right].path
		}
		return violations[left].line < violations[right].line
	})
	if len(violations) > 0 {
		for _, current := range violations {
			fmt.Fprintf(os.Stderr, "%s:%d: %s\n", current.path, current.line, current.message)
		}
		fmt.Fprintf(os.Stderr, "FAIL: %d public any contract violation(s).\n", len(violations))
		os.Exit(1)
	}
	fmt.Println("OK: public any contract preserved.")
}

func scanScope(scope string) ([]violation, error) {
	info, err := os.Stat(scope)
	if err != nil {
		return nil, fmt.Errorf("required scope %q is missing: %w", scope, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("required scope %q is not a directory", scope)
	}

	rootProductionFiles := 0
	var violations []violation
	err = filepath.WalkDir(scope, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if filepath.Dir(path) == scope {
			rootProductionFiles++
		}
		current, err := scanFile(path)
		if err != nil {
			return err
		}
		violations = append(violations, current...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if rootProductionFiles == 0 {
		return nil, fmt.Errorf("required scope %q has no production Go files", scope)
	}
	return violations, nil
}

func scanFile(path string) ([]violation, error) {
	source, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, source, parser.AllErrors)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	lines := bytes.Split(source, []byte("\n"))
	violations := scanExportedFunctions(path, fileSet, file, lines)
	violations = append(violations, scanExportedStructs(path, fileSet, file, lines)...)
	return violations, nil
}

func scanExportedFunctions(path string, fileSet *token.FileSet, file *ast.File, lines [][]byte) []violation {
	var violations []violation
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || !function.Name.IsExported() || function.Type.Results == nil {
			continue
		}
		if !fieldListContainsSliceAny(function.Type.Results) {
			continue
		}
		line := fileSet.Position(function.Pos()).Line
		if hasImmediateTag(lines, line, "//venat:allow-public-any") {
			continue
		}
		violations = append(violations, violation{path: path, line: line, message: "exported function returns []any without //venat:allow-public-any"})
	}
	return violations
}

func scanExportedStructs(path string, fileSet *token.FileSet, file *ast.File, lines [][]byte) []violation {
	var violations []violation
	typeDecls := collectFileTypeDecls(file)
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.TYPE {
			continue
		}
		for _, specification := range general.Specs {
			typeSpec, ok := specification.(*ast.TypeSpec)
			if !ok || !typeSpec.Name.IsExported() {
				continue
			}
			structure, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}
			violations = append(violations, scanExportedFields(path, fileSet, structure, lines, typeDecls)...)
		}
	}
	return violations
}

func collectFileTypeDecls(file *ast.File) map[string]ast.Expr {
	decls := make(map[string]ast.Expr)
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.TYPE {
			continue
		}
		for _, specification := range general.Specs {
			if typeSpec, ok := specification.(*ast.TypeSpec); ok && typeSpec.Name != nil {
				decls[typeSpec.Name.Name] = typeSpec.Type
			}
		}
	}
	return decls
}

func scanExportedFields(path string, fileSet *token.FileSet, structure *ast.StructType, lines [][]byte, typeDecls map[string]ast.Expr) []violation {
	var violations []violation
	for _, field := range structure.Fields.List {
		if !hasExportedOrEmbeddedField(field) || !containsAnyWithTypes(field.Type, typeDecls, map[string]bool{}) {
			continue
		}
		line := fileSet.Position(field.Pos()).Line
		if hasImmediateTag(lines, line, "// godoc-allow-any") {
			continue
		}
		violations = append(violations, violation{path: path, line: line, message: "exported field contains any without // godoc-allow-any"})
	}
	return violations
}

func hasExportedOrEmbeddedField(field *ast.Field) bool {
	if len(field.Names) == 0 {
		return true
	}
	for _, name := range field.Names {
		if name.IsExported() {
			return true
		}
	}
	return false
}

func fieldListContainsSliceAny(fields *ast.FieldList) bool {
	for _, field := range fields.List {
		found := false
		ast.Inspect(field.Type, func(node ast.Node) bool {
			array, ok := node.(*ast.ArrayType)
			if !ok || array.Len != nil {
				return true
			}
			if isLooseAny(array.Elt) {
				found = true
				return false
			}
			return true
		})
		if found {
			return true
		}
	}
	return false
}

func containsAnyWithTypes(expression ast.Expr, typeDecls map[string]ast.Expr, visited map[string]bool) bool {
	found := false
	ast.Inspect(expression, func(node ast.Node) bool {
		if expr, ok := node.(ast.Expr); ok && isLooseAny(expr) {
			found = true
			return false
		}
		ident, ok := node.(*ast.Ident)
		if !ok || visited[ident.Name] {
			return true
		}
		decl, exists := typeDecls[ident.Name]
		if !exists {
			return true
		}
		visited[ident.Name] = true
		if structType, ok := decl.(*ast.StructType); ok {
			for _, field := range structType.Fields.List {
				if hasExportedOrEmbeddedField(field) && containsAnyWithTypes(field.Type, typeDecls, visited) {
					found = true
					return false
				}
			}
			return true
		}
		if containsAnyWithTypes(decl, typeDecls, visited) {
			found = true
			return false
		}
		return true
	})
	return found
}

// isLooseAny reports the predeclared any and its spelling interface{}.
func isLooseAny(expression ast.Expr) bool {
	switch typed := expression.(type) {
	case *ast.Ident:
		return typed.Name == "any"
	case *ast.InterfaceType:
		return typed.Methods == nil || len(typed.Methods.List) == 0
	}
	return false
}

// hasImmediateTag accepts only a standalone comment line directly above the
// declaration that starts with the tag (optionally followed by ": reason"), so
// a trailing comment on a neighboring field cannot exempt the next one.
func hasImmediateTag(lines [][]byte, declarationLine int, tag string) bool {
	previous := declarationLine - 2
	if previous < 0 || previous >= len(lines) {
		return false
	}
	comment := strings.TrimSpace(string(lines[previous]))
	return comment == tag || strings.HasPrefix(comment, tag+":")
}
