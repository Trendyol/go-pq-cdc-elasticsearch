package elasticsearch

import "encoding/json"

type ActionType string

const (
	Index        ActionType = "Index"
	Delete       ActionType = "Delete"
	ScriptUpdate ActionType = "ScriptUpdate"
)

type Action struct {
	Routing   *string
	IndexName string
	Type      ActionType
	Source    []byte
	ID        []byte
}

type Script struct {
	Params map[string]interface{} `json:"params,omitempty"`
	Source string                 `json:"source"`
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

func NewScriptUpdateAction(id []byte, script Script, routing *string) Action {
	scriptBytes, _ := json.Marshal(script)
	return Action{
		ID:      id,
		Type:    ScriptUpdate,
		Source:  scriptBytes,
		Routing: routing,
	}
}
