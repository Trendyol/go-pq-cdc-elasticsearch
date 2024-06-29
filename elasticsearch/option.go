package elasticsearch

type Option func(*Bulk)

type Options []Option

func (ops Options) Apply(c *Bulk) {
	for _, op := range ops {
		op(c)
	}
}

func WithResponseHandler(respHandler ResponseHandler) Option {
	return func(b *Bulk) {
		b.responseHandler = respHandler
		b.responseHandler.OnInit(&ResponseHandlerInitContext{
			Config:              b.config,
			ElasticsearchClient: b.esClient,
		})
	}
}
