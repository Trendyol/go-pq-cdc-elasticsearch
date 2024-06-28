package elasticsearch

type ActionType string

const (
	Index  ActionType = "Index"
	Delete ActionType = "Delete"
)

type Action struct {
	Routing   *string
	Type      ActionType
	IndexName string
	Source    []byte
	ID        []byte
}

func NewDeleteAction(key []byte, routing *string) Action {
	return Action{
		ID:      key,
		Routing: routing,
		Type:    Delete,
	}
}

func NewIndexAction(key []byte, source []byte, routing *string) Action {
	return Action{
		ID:      key,
		Routing: routing,
		Source:  source,
		Type:    Index,
	}
}
