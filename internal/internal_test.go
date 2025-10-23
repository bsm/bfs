package internal_test

import (
	"testing"

	"github.com/bsm/bfs/internal"
)

func TestWithinNamespace(t *testing.T) {
	examples := []struct {
		Desc string
		Name string
		Exp  string
	}{
		{"blank", "", "/my/root"},
		{"relative", "file/name.txt", "/my/root/file/name.txt"},
		{"absolute", "/file/name.txt", "/my/root/file/name.txt"},
		{"dirty", "//file//name.txt", "/my/root/file/name.txt"},
		{"with parent references", "/file/../name.txt", "/my/root/name.txt"},
		{"escape attempts", "../file/secret.txt", "/my/root/file/secret.txt"},
		{"clever escape attempts", "/file/../../../../secret.txt", "/my/root/secret.txt"},
	}

	for _, example := range examples {
		t.Run(example.Desc, func(t *testing.T) {
			if got := internal.WithinNamespace("/my/root", example.Name); got != example.Exp {
				t.Errorf("Expected %q, got %q", example.Exp, got)
			}
		})
	}
}
