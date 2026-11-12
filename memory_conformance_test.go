package durable_test

import (
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/storagetest"
)

func TestMemoryStorageConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) durable.Storage { return durable.NewMemoryStorage() })
}
