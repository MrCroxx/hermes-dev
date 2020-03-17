package component

import "time"

type MetaCMDTYPE int
type DataCMDTYPE int

const (
	MetaCMDTYPE_RAFT_ADDZONE MetaCMDTYPE = iota
	MetaCMDTYPE_RAFT_NOTIFY_LEADERSHIP
	MetaCMDTYPE_RAFT_TRANSFER_LEADERATHIP
	MetaCMDTYPE_NODE_HEARTBEAT

	DataCMDTYPE_APPEND DataCMDTYPE = iota
)

type MetaCMD struct {
	Type      MetaCMDTYPE
	Records   []RaftRecord
	ZoneID    uint64
	NodeID    uint64
	PodID     uint64
	OldNodeID uint64
	Time      time.Time
}

type DataCMD struct {
	Type DataCMDTYPE
	Data []string
}
