package component

import (
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/pkg"
	"mrcroxx.io/hermes/transport"
	"path"
	"sync"
	"time"
)

type DataNode interface {
	Stop()
}

type dataNode struct {
	ds          DataStore
	zoneID      uint64
	nodeID      uint64
	storageDir  string
	doLead      func(old uint64)
	proposeC    chan<- []byte
	confchangeC chan<- raftpb.ConfChange
	snapshotter *snap.Snapshotter
	mux         sync.RWMutex
	advanceC    chan<- struct{}
	hbTicker    *time.Ticker
	heartbeat   func(nodeID uint64)
	peers       map[uint64]uint64
	transport   transport.Transport
}

type DataNodeConfig struct {
	ZoneID                  uint64
	NodeID                  uint64
	Peers                   map[uint64]uint64
	Join                    bool
	StorageDir              string
	TriggerSnapshotEntriesN uint64
	SnapshotCatchUpEntriesN uint64
	Transport               transport.Transport
	NotifyLeaderShip        func(nodeID uint64)
	Heartbeat               func(nodeID uint64)
}

func NewDataNode(cfg DataNodeConfig) DataNode {

	proposeC := make(chan []byte)
	confchangeC := make(chan raftpb.ConfChange)

	d := &dataNode{
		ds:          NewDataStore(path.Join(cfg.StorageDir, "block")),
		zoneID:      cfg.ZoneID,
		nodeID:      cfg.NodeID,
		storageDir:  cfg.StorageDir,
		proposeC:    proposeC,
		confchangeC: confchangeC,
		heartbeat:   cfg.Heartbeat,
		peers:       cfg.Peers,
		transport:   cfg.Transport,
	}

	speers := []uint64{}
	for pid, nid := range cfg.Peers {
		err := cfg.Transport.AddNode(pid, nid)
		if err != nil {
			log.ZAPSugaredLogger().Errorf("Error raised when add meta node peers to transport, err=%s.", err)
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
		NotifyLeadership:        cfg.NotifyLeaderShip,
		GetSnapshot:             d.getSnapshot,
	})

	d.doLead = re.DoLead
	d.snapshotter = <-re.SnapshotterReadyC
	d.advanceC = re.AdvanceC
	d.hbTicker = time.NewTicker(time.Second * 3)

	go d.readCommits(re.CommitC, re.ErrorC)
	go d.tickHeartbeat()

	return d
}

func (d *dataNode) Stop() {
	d.hbTicker.Stop()
	close(d.proposeC)
	close(d.confchangeC)
	for _, nid := range d.peers {
		if err := d.transport.RemoveNode(nid); err != nil {
			log.ZAPSugaredLogger().Errorf("Error raised when remove node from transport, err=%s.", err)
		}
	}
}

func (d *dataNode) handleDataCMD(cmd DataCMD) {
	switch cmd.Type {
	case DataCMDTYPE_APPEND:

	}
}

func (d *dataNode) propose(cmd DataCMD) {
	data, _ := pkg.Encode(cmd)
	d.proposeC <- data
}

func (d *dataNode) tickHeartbeat() {
	for _ = range d.hbTicker.C {
		d.heartbeat(d.nodeID)
	}
}

func (d *dataNode) readCommits(commitC <-chan *[]byte, errorC <-chan error) {
	for commit := range commitC {
		switch commit {
		case nil:
			snapshot, err := d.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				log.ZAPSugaredLogger().Debugf("No snapshot, done replaying log, new data incoming.")
				continue
			}
			if err != nil && err != snap.ErrNoSnapshot {
				log.ZAPSugaredLogger().Fatalf("Error raised when loading snapshot, err=%s.", err)
				d.Stop()
				return
			}
			log.ZAPSugaredLogger().Infof("Loading snapshot at term [%d] and index [%d]", snapshot.Metadata.Term, snapshot.Metadata.Index)
			var sn []RaftRecord
			err = pkg.Decode(snapshot.Data, &sn)
			if err != nil {
				log.ZAPSugaredLogger().Fatalf("Error raised when decoding snapshot, err=%s.", err)
				d.Stop()
				return
			}
			err = d.recoverFromSnapshot(snapshot.Data)
			if err != nil {
				log.ZAPSugaredLogger().Fatalf("Error raised when recovering from snapshot, err=%s.", err)
				d.Stop()
				return
			}
			log.ZAPSugaredLogger().Infof("Finish loading snapshot.")
		default:
			d.mux.Lock()
			var dataCMD DataCMD
			err := pkg.Decode(*commit, &dataCMD)
			if err != nil {
				log.ZAPSugaredLogger().Errorf("Error raised when decoding commit, err=%s.", err)
				d.Stop()
				return
			}
			log.ZAPSugaredLogger().Infof("apply cmd to MetaNode : %+v", dataCMD)
			d.handleDataCMD(dataCMD)
			d.mux.Unlock()
			d.advanceC <- struct{}{}
		}
	}
	if err, ok := <-errorC; ok {
		log.ZAPSugaredLogger().Fatalf("Error raised from raft engine, err=%s.", err)
		d.Stop()
		return
	}
}

func (d *dataNode) getSnapshot() ([]byte, error) {
	d.mux.Lock()
	defer d.mux.Unlock()
	return d.ds.GetSnapshot()
}

func (d *dataNode) recoverFromSnapshot(snap []byte) error {
	d.mux.Lock()
	defer d.mux.Unlock()
	return d.ds.RecoverFromSnapshot(snap)
}
