package component

import (
	"mrcroxx.io/hermes/pkg"
)

type RaftRecord struct {
	ZoneID   uint64
	NodeID   uint64
	PodID    uint64
	IsLeader bool
}

type RaftTable struct {
	records []RaftRecord
}

func NewRaftTable() *RaftTable {
	return &RaftTable{records: []RaftRecord{}}
}

func (rt *RaftTable) All() (result []RaftRecord) {
	result = append(result, rt.records[:]...)
	return result
}

func (rt *RaftTable) Query(condition func(rr RaftRecord) bool) (result []RaftRecord) {
	result = []RaftRecord{}
	for _, rr := range rt.records {
		if condition(rr) {
			result = append(result, rr)
		}
	}
	return result
}

func (rt *RaftTable) Insert(rrs []RaftRecord) int {
	rt.records = append(rt.records, rrs...)
	return len(rrs)
}

func (rt *RaftTable) InsertIfNotExist(rrs []RaftRecord, condition func(rr RaftRecord) bool) int {
	exists := false
	for _, rr := range rt.records {
		if condition(rr) {
			exists = true
			break
		}
	}
	if exists {
		return 0
	}
	rt.records = append(rt.records, rrs...)
	return len(rrs)
}

func (rt *RaftTable) Delete(condition func(rr RaftRecord) bool) int {
	n := 0
	for i := 0; i < len(rt.records); {
		if condition(rt.records[i]) {
			rt.records = append(rt.records[:i], rt.records[i+1:]...)
			n++
		} else {
			i++
		}
	}
	return n
}

func (rt *RaftTable) Update(condition func(rr RaftRecord) bool, update func(rr *RaftRecord)) int {
	n := 0
	for i, rr := range rt.records {
		if condition(rr) {
			update(&rt.records[i])
			n++
		}
	}
	return n
}

func (rt *RaftTable) GetSnapshot() ([]byte, error) {
	return pkg.Encode(rt.records)
}

func (rt *RaftTable) RecoverFromSnapshot(snap []byte) error {
	return pkg.Decode(snap, &rt.records)
}
