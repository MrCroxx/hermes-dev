package component

import (
	"errors"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"mrcroxx.io/hermes/cmd"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/pkg"
	"mrcroxx.io/hermes/store"
	"mrcroxx.io/hermes/transport"
	"mrcroxx.io/hermes/unit"
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

type metaNode struct {
	rt                   *store.RaftTable
	zoneID               uint64
	nodeID               uint64
	podID                uint64
	storageDir           string
	doLeadershipTransfer func(podID uint64, old uint64, transferee uint64)
	doLead               func(old uint64)
	proposeC             chan<- []byte
	confchangeC          chan<- raftpb.ConfChange
	snapshotter          *snap.Snapshotter
	mux                  sync.RWMutex
	advanceC             chan<- struct{}
	startDataNode        func(zoneID uint64, nodeID uint64, peers map[uint64]uint64)
	hbTicker             *time.Ticker
	nWakeUpTick          uint64
}

type MetaNodeConfig struct {
	ZoneID                  uint64
	NodeID                  uint64
	PodID                   uint64
	Peers                   map[uint64]uint64
	Join                    bool
	StorageDir              string
	TriggerSnapshotEntriesN uint64
	SnapshotCatchUpEntriesN uint64
	Transport               transport.Transport
	DoLeadershipTransfer    func(podID uint64, old uint64, transferee uint64)
	StartDataNode           func(zoneID uint64, nodeID uint64, peers map[uint64]uint64)
}

func NewMetaNode(cfg MetaNodeConfig) unit.MetaNode {
	proposeC := make(chan []byte)
	confchangeC := make(chan raftpb.ConfChange)

	m := &metaNode{
		rt:                   store.NewRaftTable(),
		zoneID:               cfg.ZoneID,
		nodeID:               cfg.NodeID,
		podID:                cfg.PodID,
		storageDir:           cfg.StorageDir,
		doLeadershipTransfer: cfg.DoLeadershipTransfer,
		startDataNode:        cfg.StartDataNode,
		nWakeUpTick:          3,
	}

	speers := []uint64{}
	for pid, nid := range cfg.Peers {
		err := cfg.Transport.AddNode(pid, nid)
		if err != nil {
			log.ZAPSugaredLogger().Fatalf("Error raised when add metaNode node peers to transport, err=%s.", err)
			return nil
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
		MetaNode:                m,
	})
	m.doLead = re.DoLead
	//m.mux = re.Mux
	m.snapshotter = <-re.SnapshotterReadyC
	m.proposeC = proposeC
	m.advanceC = re.AdvanceC
	m.hbTicker = time.NewTicker(time.Second * 3)

	// m.readCommits(re.CommitC, re.ErrorC)
	go m.readCommits(re.CommitC, re.ErrorC)
	go m.tickHeartbeat()

	// may init metaNode zone in raft table

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
		func(rr store.RaftRecord) bool {
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
	rs := []store.RaftRecord{}
	for nid, pid := range nodes {
		rs = append(rs, store.RaftRecord{
			ZoneID:   zoneID,
			NodeID:   nid,
			PodID:    pid,
			IsLeader: false,
		})
	}
	m.propose(cmd.MetaCMD{
		Type:    cmd.METACMDTYPE_RAFT_ADDZONE,
		ZoneID:  zoneID,
		Records: rs,
	})
	return nil
}

func (m *metaNode) TransferLeadership(zoneID uint64, nodeID uint64) error {
	// confirm node (zone id, node id) exists, and get its record
	rs := m.rt.Query(
		func(rr store.RaftRecord) bool {
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
		func(rr store.RaftRecord) bool {
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
	m.propose(cmd.MetaCMD{
		Type:      cmd.METACMDTYPE_RAFT_TRANSFER_LEADERATHIP,
		ZoneID:    r.ZoneID,
		NodeID:    r.NodeID,
		PodID:     r.PodID,
		OldNodeID: oldLeaderID,
		Time:      time.Now().Add(time.Second * 10),
	})
	return nil
}

func (m *metaNode) NotifyLeadership(nodeID uint64) {
	rs := m.rt.Query(
		func(rr store.RaftRecord) bool {
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
	m.propose(cmd.MetaCMD{
		Type:   cmd.METACMDTYPE_RAFT_NOTIFY_LEADERSHIP,
		ZoneID: r.ZoneID,
		NodeID: r.NodeID,
	})
}

func (m *metaNode) Heartbeat(nodeID uint64, extra []byte) {
	m.propose(cmd.MetaCMD{
		Type:   cmd.METACMDTYPE_NODE_HEARTBEAT,
		NodeID: nodeID,
		Time:   time.Now(),
		Extra:  extra,
	})
}

func (m *metaNode) LookUpLeader(zoneID uint64) (uint64, uint64) {
	m.mux.Lock()
	defer m.mux.Unlock()
	rrs := m.rt.Query(func(rr store.RaftRecord) bool {
		if rr.ZoneID == zoneID && rr.IsLeader {
			return true
		}
		return false
	})
	if len(rrs) == 1 {
		return rrs[0].NodeID, rrs[0].PodID
	}
	return 0, 0
}

func (m *metaNode) All() []store.RaftRecord {
	return m.rt.All()
}

func (m *metaNode) DoLead(old uint64) { m.doLead(old) }

func (m *metaNode) Stop() {
	close(m.proposeC)
	close(m.confchangeC)
	m.hbTicker.Stop()
}

// WakeUp will wake up data nodes that ain't heartbeat for a while.
func (m *metaNode) WakeUp() {
	log.ZAPSugaredLogger().Debugf("MetaNode.WakeUp is called, waking up dead data nodes in this pod.")
	m.mux.Lock()
	defer m.mux.Unlock()
	for _, rr := range m.rt.Query(func(rr store.RaftRecord) bool {
		tdead := time.Now().Add(-time.Second * 30)
		// TODO : is zero ! if create failed !
		if rr.ZoneID != m.zoneID && rr.PodID == m.podID && rr.Heartbeat.Before(tdead) && !rr.Heartbeat.IsZero() {
			return true
		}
		return false
	}) {
		go m.wakeUpNode(rr)
	}
}

func (m *metaNode) WakeUpNode(nodeID uint64) {
	m.mux.Lock()
	defer m.mux.Unlock()
	for _, rr := range m.rt.Query(func(rr store.RaftRecord) bool {
		tdead := time.Now().Add(-time.Second * 30)
		if rr.NodeID == nodeID && rr.Heartbeat.Before(tdead) {
			return true
		}
		return false
	}) {
		go m.wakeUpNode(rr)
	}
}

func (m *metaNode) wakeUpNode(rr store.RaftRecord) {
	peerRRs := m.rt.Query(func(prr store.RaftRecord) bool {
		if prr.ZoneID == rr.ZoneID {
			return true
		}
		return false
	})
	peers := make(map[uint64]uint64)
	for _, prr := range peerRRs {
		peers[prr.PodID] = prr.NodeID
	}
	log.ZAPSugaredLogger().Infof("wake up : %d", rr.NodeID)
	m.startDataNode(rr.ZoneID, rr.NodeID, peers)
}

func (m *metaNode) tickHeartbeat() {
	for _ = range m.hbTicker.C {
		m.Heartbeat(m.nodeID, nil)
		m.nWakeUpTick--
		if m.nWakeUpTick == 0 {
			m.nWakeUpTick = 10
			go m.WakeUp()
		}
	}
}

// basic methods

func (m *metaNode) handleMetaCMD(metaCMD cmd.MetaCMD) {
	switch metaCMD.Type {
	case cmd.METACMDTYPE_RAFT_ADDZONE:
		// only check if zone id exists, other checks in MetaNode.AddRaftZone
		ins := m.rt.InsertIfNotExist(
			metaCMD.Records,
			func(rr store.RaftRecord) bool {
				if rr.ZoneID == metaCMD.ZoneID {
					return true
				}
				return false
			},
		)
		if ins == 0 {
			break
		}
		diz := false
		peers := make(map[uint64]uint64)
		zid := uint64(0)
		nid := uint64(0)
		for _, rr := range metaCMD.Records {
			peers[rr.PodID] = rr.NodeID
			zid = rr.ZoneID
			if rr.ZoneID != m.zoneID && rr.PodID == m.podID {
				diz = true
				nid = rr.NodeID
			}
		}
		if !diz {
			break
		}
		m.startDataNode(zid, nid, peers)

	case cmd.METACMDTYPE_RAFT_TRANSFER_LEADERATHIP:
		if time.Now().Before(metaCMD.Time) {
			m.doLeadershipTransfer(metaCMD.PodID, metaCMD.OldNodeID, metaCMD.NodeID)
		}
	case cmd.METACMDTYPE_RAFT_NOTIFY_LEADERSHIP:
		m.rt.Update(
			func(rr store.RaftRecord) bool {
				if rr.ZoneID == metaCMD.ZoneID {
					return true
				}
				return false
			},
			func(rr *store.RaftRecord) {
				if rr.NodeID == metaCMD.NodeID {
					rr.IsLeader = true
				} else {
					rr.IsLeader = false
				}
			},
		)
	case cmd.METACMDTYPE_NODE_HEARTBEAT:
		m.rt.Update(
			func(rr store.RaftRecord) bool {
				if rr.NodeID == metaCMD.NodeID {
					return true
				}
				return false
			},
			func(rr *store.RaftRecord) {
				rr.Heartbeat = metaCMD.Time
				if rr.ZoneID != m.zoneID {
					rr.Extra = string(metaCMD.Extra)
				}
			},
		)
	}
}

func (m *metaNode) propose(cmd cmd.MetaCMD) {
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
			var sn []store.RaftRecord
			err = pkg.Decode(snapshot.Data, &sn)
			if err != nil {
				log.ZAPSugaredLogger().Fatalf("Error raised when decoding snapshot, err=%s.", err)
				panic(err)
			}
			err = m.recoverFromSnapshot(snapshot.Data)
			if err != nil {
				log.ZAPSugaredLogger().Fatalf("Error raised when recovering from snapshot, err=%s.", err)
				panic(err)
			}
			log.ZAPSugaredLogger().Infof("Finish loading snapshot.")

		default:
			m.mux.Lock()
			var metaCMD cmd.MetaCMD
			err := pkg.Decode(*commit, &metaCMD)
			if err != nil {
				log.ZAPSugaredLogger().Errorf("Error raised when decoding commit, err=%s.", err)
				panic(err)
			}
			//log.ZAPSugaredLogger().Infof("apply cmd to MetaNode : %+v", metaCMD)
			m.handleMetaCMD(metaCMD)
			m.mux.Unlock()
			m.advanceC <- struct{}{}
		}

	}
	if err, ok := <-errorC; ok {
		log.ZAPSugaredLogger().Fatalf("Error raised from raft engine, err=%s.", err)
		panic(err)
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
