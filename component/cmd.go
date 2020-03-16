package component

import "time"

type MetaCMDTYPE int

const (
	MetaCMDTYPE_ADD_ZONE = iota
	MetaCMDTYPE_RAFT_NOTIFY_LEADERSHIP
	MetaCMDTYPE_RAFT_TRANSFER_LEADERATHIP
)

type MetaCMD struct {
	Type       MetaCMDTYPE
	Records    []RaftRecord
	ZoneID     uint64
	NodeID     uint64
	PodID      uint64
	OldNodeID  uint64
	ExpireTime time.Time
}
