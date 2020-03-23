package cmd

import (
	"mrcroxx.io/hermes/store"
	"time"
)

type METACMDTYPE int
type DATACMDTYPE int
type HERMESCMDTYPE int

var (
	HEIHEIHEI = "asd"
)

const (
	METACMDTYPE_RAFT_ADDZONE METACMDTYPE = iota
	METACMDTYPE_RAFT_NOTIFY_LEADERSHIP
	METACMDTYPE_RAFT_TRANSFER_LEADERATHIP
	METACMDTYPE_NODE_HEARTBEAT
)
const (
	DATACMDTYPE_APPEND DATACMDTYPE = iota
	DATACMDTYPE_CACHE
	DATACMDTYPE_PERSIST
)
const (
	HERMESCMDTYPE_APPEND HERMESCMDTYPE = iota
)

type MetaCMD struct {
	Type      METACMDTYPE
	Records   []store.RaftRecord
	ZoneID    uint64
	NodeID    uint64
	PodID     uint64
	OldNodeID uint64
	Time      time.Time
	Extra     []byte
}

type DataCMD struct {
	Type DATACMDTYPE
	Data []string
	TS   int64
	N    uint64
}

// producer (push)
// consumer (push)

type HermesProducerCMD struct {
	Type   HERMESCMDTYPE
	ZoneID uint64
	NodeID uint64
	TS     int64
	Data   []string
}

type HermesProducerRSP struct {
	Err    string // error
	TS     int64
	NodeID uint64 // leader id now for client to redirect
	PodID  uint64 // pod id for leader node now
}

type HermesConsumerCMD struct {
	ZoneID     uint64   `json:"zone_id"`
	FirstIndex uint64   `json:"first_index"`
	Data       []string `json:"data"`
}

type HermesConsumerRSP struct {
	ACK uint64 `json:"ack"`
}
