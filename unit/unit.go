package unit

import "mrcroxx.io/hermes/store"

type Pod interface {
	Stop()

	AddRaftZone(zoneID uint64, nodes map[uint64]uint64) error
	TransferLeadership(zoneID uint64, nodeID uint64) error
	WakeUpNode(nodeID uint64)

	All() ([]store.RaftRecord, error)
}

type MetaNode interface {
	NodeID() uint64
	AddRaftZone(zoneID uint64, nodes map[uint64]uint64) error
	TransferLeadership(zoneID uint64, nodeID uint64) error
	NotifyLeadership(nodeID uint64)
	All() []store.RaftRecord
	LookUpLeader(zoneID uint64) uint64
	DoLead(old uint64)
	Heartbeat(nodeID uint64, extra []byte)
	WakeUp()
	WakeUpNode(nodeID uint64)
	Stop()
}

type DataNode interface {
	Append(firstIndex uint64, vs []string)
	ACK() uint64
	Metadata() []byte
	DoLead(old uint64)
	Stop()
}
