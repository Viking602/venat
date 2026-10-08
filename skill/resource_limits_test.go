package skill

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResource_BoundsEmptyDirectoriesAndDepth(t *testing.T) {
	t.Run("entries", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "entries")
		writeTestSkill(t, dir, "entries", "Body")
		for i := 0; i < maxResourceEntries; i++ {
			if err := os.Mkdir(filepath.Join(dir, fmt.Sprintf("d%04d", i)), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := LoadDir(dir); err == nil || !strings.Contains(err.Error(), "entry limit") {
			t.Fatalf("LoadDir entry limit = %v", err)
		}
	})
	t.Run("depth", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "depth")
		writeTestSkill(t, dir, "depth", "Body")
		path := dir
		for i := 0; i <= maxResourceDepth; i++ {
			path = filepath.Join(path, "nested")
		}
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadDir(dir); err == nil || !strings.Contains(err.Error(), "depth") {
			t.Fatalf("LoadDir depth limit = %v", err)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		root, err := os.OpenRoot(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		walk := resourceTraversal{root: root, deadline: time.Now().Add(-time.Second)}
		var resources []Resource
		if err := walk.directory(".", 0, &resources); err == nil || !strings.Contains(err.Error(), "deadline") {
			t.Fatalf("deadline = %v", err)
		}
	})
}
