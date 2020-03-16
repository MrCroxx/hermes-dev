package component

type NodeType string

const (
	TMetaNode NodeType = "MetaNode"
	TDataNode NodeType = "DataNode"
)

type Node interface {
	NodeType() NodeType
	GetSnapshot([]byte, error)  // func to snapshot node state machine
	ApplySnapshot([]byte) error // func to restore node state machine from snapshot
}
