package storage

import (
	"context"
	"path/filepath"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/storagetest"
)

func TestJSONLConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) durable.Storage {
		st, err := OpenJSONL(context.Background(), t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return st
	})
}

func TestSQLiteConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) durable.Storage {
		st, err := OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "conformance.db"))
		if err != nil {
			t.Fatal(err)
		}
		return st
	})
}
