package client

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
)

func TestNewTransport_MaxIdemponentCallAttempts(t *testing.T) {
	t.Run("zero defaults to one", func(t *testing.T) {
		tr := NewTransport(config.Elasticsearch{})
		transport, ok := tr.(*transport)
		if !ok {
			t.Fatalf("expected *transport, got %T", tr)
		}
		if transport.client.MaxIdemponentCallAttempts != 1 {
			t.Fatalf("expected MaxIdemponentCallAttempts 1, got %d", transport.client.MaxIdemponentCallAttempts)
		}
	})

	t.Run("explicit value", func(t *testing.T) {
		tr := NewTransport(config.Elasticsearch{MaxIdemponentCallAttempts: 3})
		transport, ok := tr.(*transport)
		if !ok {
			t.Fatalf("expected *transport, got %T", tr)
		}
		if transport.client.MaxIdemponentCallAttempts != 3 {
			t.Fatalf("expected MaxIdemponentCallAttempts 3, got %d", transport.client.MaxIdemponentCallAttempts)
		}
	})
}
