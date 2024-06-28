package cdc

import "github.com/prometheus/client_golang/prometheus"

type Option func(Connector)

type Options []Option

func (ops Options) Apply(c Connector) {
	for _, op := range ops {
		op(c)
	}
}

func WithResponseHandler(respHandler any) Option {
	return func(c Connector) {
		c.(*connector).responseHandler = respHandler
	}
}

func WithPrometheusMetrics(collectors []prometheus.Collector) Option {
	return func(c Connector) {
		// TODO: metrics
	}
}
