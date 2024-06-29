package elasticsearch

import (
	"github.com/Trendyol/go-pq-cdc-elasticsearch/config"
	es "github.com/elastic/go-elasticsearch/v7"
)

type ResponseHandler interface {
	OnSuccess(ctx *ResponseHandlerContext)
	OnError(ctx *ResponseHandlerContext)
	OnInit(ctx *ResponseHandlerInitContext)
}

type ResponseHandlerContext struct {
	Action *Action
	Err    error
}

type ResponseHandlerInitContext struct {
	Config              *config.Config
	ElasticsearchClient *es.Client
}
