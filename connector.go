package gopqcdcelasticsearch

type Action struct{}

type Message struct{}

type Handler func(msg Message) []Action

type Connector struct {
}

func NewConnector(config any, handler Handler, options ...func(connector *Connector)) (*Connector, error) {
	return nil, nil
}
