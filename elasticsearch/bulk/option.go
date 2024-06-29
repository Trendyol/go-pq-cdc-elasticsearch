package bulk

import (
	"github.com/Trendyol/go-pq-cdc-elasticsearch/elasticsearch"
)

type Option func(*Bulk)

type Options []Option

func (ops Options) Apply(c *Bulk) {
	for _, op := range ops {
		op(c)
	}
}

func WithResponseHandler(respHandler elasticsearch.ResponseHandler) Option {
	return func(b *Bulk) {
		if respHandler == nil {
			return
		}

		b.responseHandler = respHandler
		b.responseHandler.OnInit(&elasticsearch.ResponseHandlerInitContext{
			Config:              b.config,
			ElasticsearchClient: b.esClient,
		})
	}
}
