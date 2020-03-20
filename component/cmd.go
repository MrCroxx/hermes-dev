package component

import "time"

type METACMDTYPE int
type DATACMDTYPE int
type HERMESCMDTYPE int

const (
	METACMDTYPE_RAFT_ADDZONE METACMDTYPE = iota
	METACMDTYPE_RAFT_NOTIFY_LEADERSHIP
	METACMDTYPE_RAFT_TRANSFER_LEADERATHIP
	METACMDTYPE_NODE_HEARTBEAT

	DATACMDTYPE_APPEND DATACMDTYPE = iota

	HERMESCMDTYPE_APPEND HERMESCMDTYPE = iota
)

type MetaCMD struct {
	Type      METACMDTYPE
	Records   []RaftRecord
	ZoneID    uint64
	NodeID    uint64
	PodID     uint64
	OldNodeID uint64
	Time      time.Time
	Extra     []byte
}

type DataCMD struct {
	Type       DATACMDTYPE
	FirstIndex uint64
	Data       []string
}

type HermesCMD struct {
	Type       HERMESCMDTYPE
	ZoneID     uint64
	NodeID     uint64
	FirstIndex uint64
	Data       []string
}

type HermesRSP struct {
	Err        error  // error
	NodeID     uint64 // leader id now for client to redirect
	FirstIndex uint64 // first index applied by data node
}
