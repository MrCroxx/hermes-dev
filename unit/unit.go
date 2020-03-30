package unit

import (
	"context"
	"github.com/coreos/etcd/raft/raftpb"
	"mrcroxx.io/hermes/config"
	"mrcroxx.io/hermes/store"
)

type Metadata struct {
	Config      config.HermesConfig
	RaftRecords []store.RaftRecord
}

type Core interface {
	RaftProcessor(nodeID uint64) func(ctx context.Context, m raftpb.Message) error
	LookUpLeader(zoneID uint64) (nodeID uint64, podID uint64)
	AppendData(nodeID uint64, ts int64, data []string, callback func(int64)) bool
}

type Pod interface {
	Stop()                                                    // Stop pod gracefully.
	AddRaftZone(zoneID uint64, nodes map[uint64]uint64) error // Add new raft zone in cluster, if zone id equals meta zone id, initialize meta zone.
	TransferLeadership(zoneID uint64, nodeID uint64) error    // Transfer leadership of the target zone.
	WakeUpNode(nodeID uint64)                                 //  Wake up a node immediately. (NOT recommended, may lead to panic if node is being waked up)
	Metadata() (*Metadata, error)                             // Return meta data of the cluster.
	InitMetaZone() error                                      // Initialize meta zone gracefully.
	All() ([]store.RaftRecord, error)                         // Return all RaftRecords recorded in the meta node in this pod. (For debug only)
}

type Node interface {
	NodeID() uint64
	RaftProcessor() func(ctx context.Context, m raftpb.Message) error
	DoLead(old uint64)
	Stop()
}

type MetaNode interface {
	Node
	AddRaftZone(zoneID uint64, nodes map[uint64]uint64) error
	TransferLeadership(zoneID uint64, nodeID uint64) error
	NotifyLeadership(nodeID uint64)
	All() []store.RaftRecord
	LookUpLeader(zoneID uint64) (nodeID uint64, podID uint64)
	Heartbeat(nodeID uint64, extra []byte)
	WakeUp()
	WakeUpNode(nodeID uint64)
}

type DataNode interface {
	Node
	ProposeAppend(ts int64, vs []string)
	Metadata() []byte
	RegisterACKCallback(ts int64, callback func(ts int64))
}
