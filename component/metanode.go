package component

import (
	"errors"
	"fmt"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/pkg"
	"mrcroxx.io/hermes/transport"
	"sort"
	"sync"
	"time"
)

var (
	errKeyNotExists            = errors.New("key not exists")
	errZoneIDExists            = errors.New("zone id exists")
	errZoneIDNotExists         = errors.New("zone id not exists")
	errNodeIDExists            = errors.New("node id exists")
	errNodeIDNotExists         = errors.New("node id not exists")
	errZoneIDORNodeIDExists    = errors.New("zone id or node id exists")
	errZoneIDORNodeIDNotExists = errors.New("zone id or node id not exists")
)

type MetaNode interface {
	NodeID() uint64
	AddRaftZone(zoneID uint64, nodes map[uint64]uint64) error
	TransferLeadership(zoneID uint64, nodeID uint64) error
	NotifyLeadership(nodeID uint64)
	All() []RaftRecord
	DoLead(old uint64)
}

type metaNode struct {
	rt                   *RaftTable
	zoneID               uint64
	nodeID               uint64
	storageDir           string
	doLeadershipTransfer func(podID uint64, old uint64, transferee uint64)
	doLead               func(old uint64)
	proposeC             chan<- []byte
	confchangeC          chan<- raftpb.ConfChange
	snapshotter          *snap.Snapshotter
	mux                  sync.RWMutex
	advanceC             chan<- struct{}
}

type MetaNodeConfig struct {
	ZoneID                  uint64
	NodeID                  uint64
	Pods                    map[uint64]uint64
	Join                    bool
	StorageDir              string
	TriggerSnapshotEntriesN uint64
	SnapshotCatchUpEntriesN uint64
	Transport               transport.Transport
	DoLeadershipTransfer    func(podID uint64, old uint64, transferee uint64)
}

func NewMetaNode(cfg MetaNodeConfig) MetaNode {
	proposeC := make(chan []byte)
	confchangeC := make(chan raftpb.ConfChange)

	m := &metaNode{
		rt:                   NewRaftTable(),
		zoneID:               cfg.ZoneID,
		nodeID:               cfg.NodeID,
		storageDir:           cfg.StorageDir,
		doLeadershipTransfer: cfg.DoLeadershipTransfer,
	}

	speers := []uint64{}
	for pid, nid := range cfg.Pods {
		err := cfg.Transport.AddNode(pid, nid)
		if err != nil {
			log.ZAPSugaredLogger().Fatalf("Error raised when add meta node peers to transport, err=%s.", err)
			panic(fmt.Sprintf("Error raised when add meta node peers to transport, err=%s.", err))
		}
		speers = append(speers, nid)
	}

	re := NewRaftEngine(RaftEngineConfig{
		NodeID:                  cfg.NodeID,
		Peers:                   speers,
		Join:                    cfg.Join,
		Transport:               cfg.Transport,
		StorageDir:              cfg.StorageDir,
		ProposeC:                proposeC,
		ConfChangeC:             confchangeC,
		TriggerSnapshotEntriesN: cfg.TriggerSnapshotEntriesN,
		SnapshotCatchUpEntriesN: cfg.SnapshotCatchUpEntriesN,
		NotifyLeadership:        m.NotifyLeadership,
		GetSnapshot:             m.getSnapshot,
	})
	// cfg.Transport.BindRaft(cfg.NodeID, re.Raft)
	m.doLead = re.DoLead
	//m.mux = re.Mux
	m.snapshotter = <-re.SnapshotterReadyC
	m.proposeC = proposeC
	m.advanceC = re.AdvanceC

	// m.readCommits(re.CommitC, re.ErrorC)
	go m.readCommits(re.CommitC, re.ErrorC)

	// may init meta zone in raft table

	//metaNodes := make(map[uint64]uint64)
	//for pid, nid := range cfg.Pods {
	//	metaNodes[nid] = pid
	//}
	//_ = m.AddRaftZone(cfg.ZoneID, metaNodes)

	return m
}

// implement methods

func (m *metaNode) NodeID() uint64 {
	return m.nodeID
}

func (m *metaNode) AddRaftZone(zoneID uint64, nodes map[uint64]uint64) error {
	if rs := m.rt.Query(
		func(rr RaftRecord) bool {
			if rr.ZoneID == zoneID {
				return true
			}
			if _, ok := nodes[rr.NodeID]; ok {
				return true
			}
			return false
		},
	); len(rs) > 0 {
		return errZoneIDORNodeIDExists
	}
	rs := []RaftRecord{}
	for nid, pid := range nodes {
		rs = append(rs, RaftRecord{
			ZoneID:   zoneID,
			NodeID:   nid,
			PodID:    pid,
			IsLeader: false,
		})
	}
	m.propose(MetaCMD{
		Type:    MetaCMDTYPE_ADD_ZONE,
		ZoneID:  zoneID,
		Records: rs,
	})
	return nil
}

func (m *metaNode) TransferLeadership(zoneID uint64, nodeID uint64) error {
	// confirm node (zone id, node id) exists, and get its record
	rs := m.rt.Query(
		func(rr RaftRecord) bool {
			if rr.ZoneID == zoneID && rr.NodeID == nodeID {
				return true
			}
			return false
		},
	)
	if len(rs) == 0 {
		return errZoneIDORNodeIDNotExists
	}
	r := rs[0]
	// get old leader id
	oldrs := m.rt.Query(
		func(rr RaftRecord) bool {
			if rr.ZoneID == zoneID && rr.IsLeader {
				return true
			}
			return false
		},
	)
	oldLeaderID := uint64(0)
	if len(oldrs) > 0 {
		oldLeaderID = oldrs[0].NodeID
	}
	// propose leadership transfer
	m.propose(MetaCMD{
		Type:       MetaCMDTYPE_RAFT_TRANSFER_LEADERATHIP,
		ZoneID:     r.ZoneID,
		NodeID:     r.NodeID,
		PodID:      r.PodID,
		OldNodeID:  oldLeaderID,
		ExpireTime: time.Now().Add(time.Second * 3),
	})
	return nil
}

func (m *metaNode) NotifyLeadership(nodeID uint64) {
	rs := m.rt.Query(
		func(rr RaftRecord) bool {
			if rr.NodeID == nodeID {
				return true
			}
			return false
		},
	)
	if len(rs) == 0 {
		return
	}
	r := rs[0]
	m.propose(MetaCMD{
		Type:   MetaCMDTYPE_RAFT_NOTIFY_LEADERSHIP,
		ZoneID: r.ZoneID,
		NodeID: r.NodeID,
	})
}

func (m *metaNode) All() []RaftRecord {
	return m.rt.All()
}

func (m *metaNode) DoLead(old uint64) {
	m.doLead(old)
}

// basic methods

func (m *metaNode) handleMetaCMD(cmd MetaCMD) {
	switch cmd.Type {
	case MetaCMDTYPE_ADD_ZONE:
		// only check if zone id exists, other checks in MetaNode.AddRaftZone
		m.rt.InsertIfNotExist(
			cmd.Records,
			func(rr RaftRecord) bool {
				if rr.ZoneID == cmd.ZoneID {
					return true
				}
				return false
			},
		)
	case MetaCMDTYPE_RAFT_TRANSFER_LEADERATHIP:
		if time.Now().Before(cmd.ExpireTime) {
			m.doLeadershipTransfer(cmd.PodID, cmd.OldNodeID, cmd.NodeID)
		}
	case MetaCMDTYPE_RAFT_NOTIFY_LEADERSHIP:
		m.rt.Update(
			func(rr RaftRecord) bool {
				if rr.ZoneID == cmd.ZoneID {
					return true
				}
				return false
			},
			func(rr *RaftRecord) {
				if rr.NodeID == cmd.NodeID {
					rr.IsLeader = true
				} else {
					rr.IsLeader = false
				}
			},
		)
	}
}

func (m *metaNode) propose(cmd MetaCMD) {
	data, _ := pkg.Encode(cmd)
	m.proposeC <- data
}

func (m *metaNode) readCommits(commitC <-chan *[]byte, errorC <-chan error) {
	for commit := range commitC {
		switch commit {
		case nil:
			// done replaying log, new kvcmd incoming
			// OR
			// signaled to load snapshot
			snapshot, err := m.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				log.ZAPSugaredLogger().Debugf("No snapshot, done replaying log, new data incoming.")
				continue
			}
			if err != nil && err != snap.ErrNoSnapshot {
				log.ZAPSugaredLogger().Fatalf("Error raised when loading snapshot, err=%s.", err)
				panic(err)
			}
			log.ZAPSugaredLogger().Infof("Loading snapshot at term [%d] and index [%d]", snapshot.Metadata.Term, snapshot.Metadata.Index)
			var sn []RaftRecord
			pkg.Decode(snapshot.Data, &sn)
			log.ZAPSugaredLogger().Debugf("apply not empty snapshot : ")
			sort.Slice(sn, func(i, j int) bool {
				if sn[i].ZoneID == sn[j].ZoneID {
					return sn[i].NodeID < sn[j].NodeID
				} else {
					return sn[i].ZoneID < sn[j].ZoneID
				}
			})
			for _, r := range sn {
				log.ZAPSugaredLogger().Infof("%+v", r)
			}
			err = m.recoverFromSnapshot(snapshot.Data)
			if err != nil {
				log.ZAPSugaredLogger().Fatalf("Error raised when recovering from snapshot, err=%s.", err)
				panic(err)
			}
			log.ZAPSugaredLogger().Infof("Finish loading snapshot.")

		default:
			m.mux.Lock()
			var metaCMD MetaCMD
			err := pkg.Decode(*commit, &metaCMD)
			if err != nil {
				log.ZAPSugaredLogger().Errorf("Error raised when decoding commit, err=%s.", err)
				panic(err)
			}
			log.ZAPSugaredLogger().Infof("apply cmd to MetaNode : %+v", metaCMD)
			m.handleMetaCMD(metaCMD)
			m.mux.Unlock()
			m.advanceC <- struct{}{}
		}

	}
	if err, ok := <-errorC; ok {
		log.ZAPSugaredLogger().Fatalf("Error raised from raft engine, err=%s.", err)
		return
	}
}

func (m *metaNode) getSnapshot() ([]byte, error) {
	m.mux.Lock()
	defer m.mux.Unlock()
	return m.rt.GetSnapshot()
}

func (m *metaNode) recoverFromSnapshot(snap []byte) error {
	m.mux.Lock()
	defer m.mux.Unlock()
	return m.rt.RecoverFromSnapshot(snap)
}
