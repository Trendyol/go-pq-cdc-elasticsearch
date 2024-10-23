package bulk

import (
	"os"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch"
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "go_pq_cdc_elasticsearch"

type Metric interface {
	SetProcessLatency(latency int64)
	SetBulkRequestProcessLatency(latency int64)
	PrometheusCollectors() []prometheus.Collector
	incrementOp(opType elasticsearch.ActionType, index string)
}

var hostname, _ = os.Hostname()

type metric struct {
	slotName string
	pqCDC    cdc.Connector

	processLatencyMs            prometheus.Gauge
	bulkRequestProcessLatencyMs prometheus.Gauge

	totalIndex  map[string]prometheus.Counter
	totalDelete map[string]prometheus.Counter
}

func NewMetric(pqCDC cdc.Connector, slotName string) Metric {
	return &metric{
		slotName: slotName,
		pqCDC:    pqCDC,
		processLatencyMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "process_latency",
			Name:      "current",
			Help:      "latest elasticsearch connector process latency in nanoseconds",
			ConstLabels: prometheus.Labels{
				"host":      hostname,
				"slot_name": slotName,
			},
		}),
		bulkRequestProcessLatencyMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "bulk_request_process_latency",
			Name:      "current",
			Help:      "latest elasticsearch connector bulk request process latency in nanoseconds",
			ConstLabels: prometheus.Labels{
				"host":      hostname,
				"slot_name": slotName,
			},
		}),
		totalIndex:  make(map[string]prometheus.Counter),
		totalDelete: make(map[string]prometheus.Counter),
	}
}

func (m *metric) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.processLatencyMs,
		m.bulkRequestProcessLatencyMs,
	}
}

func (m *metric) SetProcessLatency(latency int64) {
	m.processLatencyMs.Set(float64(latency))
}

func (m *metric) SetBulkRequestProcessLatency(latency int64) {
	m.bulkRequestProcessLatencyMs.Set(float64(latency))
}

func (m *metric) incrementOp(opType elasticsearch.ActionType, index string) {
	switch opType {
	case elasticsearch.Index:
		if _, exists := m.totalIndex[index]; !exists {
			m.totalIndex[index] = prometheus.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: "index",
				Name:      "total",
				Help:      "total number of index operation in elasticsearch",
				ConstLabels: prometheus.Labels{
					"slot_name":  m.slotName,
					"index_name": index,
					"host":       hostname,
				},
			})
			m.pqCDC.SetMetricCollectors(m.totalIndex[index])
		}

		m.totalIndex[index].Add(1)
	case elasticsearch.Delete:
		if _, exists := m.totalDelete[index]; !exists {
			m.totalDelete[index] = prometheus.NewCounter(prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: "delete",
				Name:      "total",
				Help:      "total number of delete operation in elasticsearch",
				ConstLabels: prometheus.Labels{
					"slot_name":  m.slotName,
					"index_name": index,
					"host":       hostname,
				},
			})
			m.pqCDC.SetMetricCollectors(m.totalDelete[index])
		}

		m.totalDelete[index].Add(1)
	}
}
