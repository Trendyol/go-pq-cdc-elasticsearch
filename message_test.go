package cdc

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
)

func TestNewRelationMessage(t *testing.T) {
	relation := &format.Relation{
		Namespace: "public",
		Name:      "users",
		OID:       42,
	}

	message := NewRelationMessage(nil, relation)

	if !message.Type.IsRelation() {
		t.Fatalf("expected relation message type, got %q", message.Type)
	}
	if message.Relation != relation {
		t.Fatal("expected original relation metadata to be preserved")
	}
	if message.TableNamespace != relation.Namespace || message.TableName != relation.Name {
		t.Fatalf("expected table %s.%s, got %s.%s", relation.Namespace, relation.Name, message.TableNamespace, message.TableName)
	}
	if !message.EventTime.IsZero() {
		t.Fatalf("expected relation message to have no event time, got %s", message.EventTime)
	}
	if message.OldData != nil || message.NewData != nil {
		t.Fatal("expected relation message to have no row data")
	}
}
