package cdc

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
)

func TestNewMessageCarriesLSN(t *testing.T) {
	const lsn pq.LSN = 0x16B374D848

	tests := []struct {
		name    string
		message Message
	}{
		{name: "insert", message: NewInsertMessage(nil, &format.Insert{LSN: lsn})},
		{name: "update", message: NewUpdateMessage(nil, &format.Update{LSN: lsn})},
		{name: "delete", message: NewDeleteMessage(nil, &format.Delete{LSN: lsn})},
		{name: "snapshot", message: NewSnapshotMessage(nil, &format.Snapshot{LSN: lsn})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, lsn, tt.message.LSN)
		})
	}
}

func TestNewMessageCarriesTransactionID(t *testing.T) {
	const xid uint32 = 4711

	tests := []struct {
		message any
		name    string
		xid     uint32
	}{
		{name: "insert", message: &format.Insert{}, xid: xid},
		{name: "update", message: &format.Update{}, xid: xid},
		{name: "delete", message: &format.Delete{}, xid: xid},
		// Snapshot events are not part of a transaction, so the listener context carries Xid 0.
		{name: "snapshot", message: &format.Snapshot{}, xid: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, ok := newMessage(nil, &replication.ListenerContext{Message: tt.message, Xid: tt.xid})

			assert.True(t, ok)
			assert.Equal(t, tt.xid, msg.TransactionID)
		})
	}
}

func TestNewMessageReturnsFalseForUnsupportedMessage(t *testing.T) {
	msg, ok := newMessage(nil, &replication.ListenerContext{Message: &format.Truncate{}, Xid: 1})

	assert.False(t, ok)
	assert.Equal(t, Message{}, msg)
}
