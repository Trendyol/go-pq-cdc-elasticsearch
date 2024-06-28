package cdc

import (
	"context"
	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"os"
)

type Connector interface {
	Start(ctx context.Context)
	Close()
}

type connector struct {
	handler         Handler
	responseHandler any
	cfg             *config.Config

	cancelCh chan os.Signal
	readyCh  chan struct{}
}

func NewConnector(ctx context.Context, cfg config.Config, handler Handler, options ...Option) (Connector, error) {
	cfg.SetDefault()

	esConnector := &connector{
		cfg:     &cfg,
		handler: handler,
	}

	Options(options).Apply(esConnector)

	pqCDC, err := cdc.NewConnector(ctx, cfg.CDC, esConnector.listener)
	if err != nil {
		return nil, err
	}

	cfg.CDC = *pqCDC.GetConfig()

	return esConnector, nil
}

func (c *connector) Start(ctx context.Context) {}

func (c *connector) Close() {}

func (c *connector) listener(ctx *replication.ListenerContext) {
	var msg Message
	switch m := ctx.Message.(type) {
	case *format.Insert:
		msg = NewInsertMessage(m)
	case *format.Update:
		msg = NewUpdateMessage(m)
	case *format.Delete:
		msg = NewDeleteMessage(m)
	default:
		return
	}

	actions := c.handler(msg)
	if len(actions) == 0 {
		if err := ctx.Ack(); err != nil {
			logger.Error("ack", "error", err)
		}
		return
	}
}
