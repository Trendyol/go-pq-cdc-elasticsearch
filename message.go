package cdc

import (
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
)

type Message struct{}

func NewInsertMessage(m *format.Insert) Message {
	return Message{}
}

func NewUpdateMessage(m *format.Update) Message {
	return Message{}
}

func NewDeleteMessage(m *format.Delete) Message {
	return Message{}
}
