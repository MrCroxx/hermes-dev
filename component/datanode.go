package component

import (
	"encoding/json"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"io/ioutil"
	"mrcroxx.io/hermes/cmd"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/pkg"
	"mrcroxx.io/hermes/store"
	"mrcroxx.io/hermes/transport"
	"mrcroxx.io/hermes/unit"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type DataNodeMetadata struct {
	DeletedIndex   uint64 `json:"DeletedIndex"`
	PersistedIndex uint64 `json:"PersistedIndex"`
	CachedIndex    uint64 `json:"CachedIndex"`
	FreshIndex     uint64 `json:"FreshIndex"`
}

type dataNode struct {
	ds           store.DataStore
	zoneID       uint64
	nodeID       uint64
	storageDir   string
	doLead       func(old uint64)
	proposeC     chan<- []byte
	confchangeC  chan<- raftpb.ConfChange
	snapshotter  *snap.Snapshotter
	mux          sync.RWMutex
	advanceC     chan<- struct{}
	hbTicker     *time.Ticker
	heartbeat    func(nodeID uint64, extra []byte)
	peers        map[uint64]uint64
	transport    transport.Transport
	ackCallbacks map[int64]func(ts int64)
	pushDataURL  string
	maxPushN     uint64
	maxCacheN    uint64
	done         chan struct{}
	caching      int32
	persisting   int32
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
	PushDataURL             string
	MaxCacheN               uint64
	MaxPushN                uint64
	NotifyLeaderShip        func(nodeID uint64)
	Heartbeat               func(nodeID uint64, extra []byte)
}

func NewDataNode(cfg DataNodeConfig) unit.DataNode {

	proposeC := make(chan []byte)
	confchangeC := make(chan raftpb.ConfChange)
	pBLK := path.Join(cfg.StorageDir, "blk")
	if _, err := os.Stat(pBLK); err != nil {
		err = os.MkdirAll(pBLK, 0750)
		if err != nil {
			log.ZAPSugaredLogger().Errorf("Error raised when creating blk storage dir, err=%s.", err)
		}
	}

	d := &dataNode{
		ds:           store.NewDataStore(pBLK),
		zoneID:       cfg.ZoneID,
		nodeID:       cfg.NodeID,
		storageDir:   cfg.StorageDir,
		proposeC:     proposeC,
		confchangeC:  confchangeC,
		heartbeat:    cfg.Heartbeat,
		peers:        cfg.Peers,
		transport:    cfg.Transport,
		pushDataURL:  cfg.PushDataURL,
		maxPushN:     cfg.MaxPushN,
		maxCacheN:    cfg.MaxCacheN,
		done:         make(chan struct{}),
		ackCallbacks: make(map[int64]func(int64)),
		caching:      0,
		persisting:   0,
	}

	speers := []uint64{}
	for pid, nid := range cfg.Peers {
		err := cfg.Transport.AddNode(pid, nid)
		if err != nil {
			log.ZAPSugaredLogger().Errorf("Error raised when add metaNode node peers to transport, err=%s.", err)
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
		DataNode:                d,
	})

	d.doLead = re.DoLead
	d.snapshotter = <-re.SnapshotterReadyC
	d.advanceC = re.AdvanceC
	d.hbTicker = time.NewTicker(time.Second * 3)

	go d.readCommits(re.CommitC, re.ErrorC)
	go d.tickHeartbeat()

	t := time.NewTimer(time.Second * 10)
	go func() {
		<-t.C
		go d.startPushing()
		go d.startPersisting()
	}()

	return d
}

func (d *dataNode) Stop() {
	close(d.done)
	d.hbTicker.Stop()
	close(d.proposeC)
	close(d.confchangeC)
	for _, nid := range d.peers {
		if err := d.transport.RemoveNode(nid); err != nil {
			log.ZAPSugaredLogger().Errorf("Error raised when remove node from transport, err=%s.", err)
		}
	}
}

func (d *dataNode) DoLead(old uint64) { d.doLead(old) }

func (d *dataNode) Metadata() []byte {

	d.mux.Lock()
	defer d.mux.Unlock()
	di, pi, ci, fi := d.ds.Indexes()
	s, _ := json.Marshal(DataNodeMetadata{
		DeletedIndex:   di,
		PersistedIndex: pi,
		CachedIndex:    ci,
		FreshIndex:     fi,
	})
	return s
}

func (d *dataNode) ProposeAppend(ts int64, vs []string) {
	d.propose(cmd.DataCMD{
		Type: cmd.DATACMDTYPE_APPEND,
		Data: vs,
		TS:   ts,
	})
}

func (d *dataNode) proposeCache(n uint64) {
	d.propose(cmd.DataCMD{
		Type: cmd.DATACMDTYPE_CACHE,
		N:    n,
	})
}

func (d *dataNode) proposePersist(n uint64) {
	d.propose(cmd.DataCMD{
		Type: cmd.DATACMDTYPE_PERSIST,
		N:    n,
	})
}

func (d *dataNode) RegisterACKCallback(ts int64, callback func(ts int64)) {
	d.mux.Lock()
	defer d.mux.Unlock()
	d.ackCallbacks[ts] = callback

}

// internal functions

func (d *dataNode) startPushing() {
	for {
		time.Sleep(time.Millisecond * 100)
		select {
		case <-d.done:
			return
		default:
			if !d.checkLeadership() || atomic.LoadInt32(&d.caching) == 1 {
				continue
			}
			n, ack := d.pushData()
			if ack == 0 {
				continue
			}
			atomic.StoreInt32(&d.caching, 1)
			d.proposeCache(n)
		}
	}
}

func (d *dataNode) startPersisting() {
	for {
		time.Sleep(time.Millisecond * 100)
		select {
		case <-d.done:
			return
		default:
			if !d.checkLeadership() || atomic.LoadInt32(&d.persisting) == 1 {
				continue
			}
			d.mux.Lock()
			_, pi, ci, _ := d.ds.Indexes()
			d.mux.Unlock()
			nc := ci - pi
			if nc <= d.maxCacheN {
				continue
			}
			n := nc - d.maxCacheN
			atomic.StoreInt32(&d.persisting, 1)
			d.proposePersist(n)
		}
	}
}

func (d *dataNode) checkLeadership() bool {
	nid, _ := d.transport.LookUpLeader(d.zoneID)
	return nid == d.nodeID
}

// pushData push fresh data (less than MaxPushN) to consumer and returns number of pushed fresh data ahead to cache and ack to seek next push index.
func (d *dataNode) pushData() (n uint64, ack uint64) {
	d.mux.Lock()
	_, _, fi, _ := d.ds.Indexes()
	fi++
	data, n := d.ds.Get(d.maxPushN)
	d.mux.Unlock()
	bs, err := json.Marshal(cmd.HermesConsumerCMD{
		ZoneID:     d.zoneID,
		FirstIndex: fi,
		Data:       data,
	})
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when marshalling HermesConsumerCMD, err=%s.", err)
		return 0, 0
	}
	req, err := http.NewRequest("PUT", d.pushDataURL, strings.NewReader(string(bs)))
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when creating HermesConsumerCMD request, err=%s.", err)
		return 0, 0
	}
	rrsp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when sending HermesConsumerCMD request, err=%s.", err)
		return 0, 0
	}
	var rsp cmd.HermesConsumerRSP
	brsp, err := ioutil.ReadAll(rrsp.Body)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when reading HermesConsumerRSP body, err=%s.", err)
		return 0, 0
	}
	_ = rrsp.Body.Close()
	err = json.Unmarshal(brsp, &rsp)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when unmarshalling HermesConsumerRSP, err=%s.", err)
		return 0, 0
	}
	return n, rsp.ACK
}

func (d *dataNode) handleDataCMD(dataCMD cmd.DataCMD) {
	switch dataCMD.Type {
	case cmd.DATACMDTYPE_APPEND:
		d.ds.Append(dataCMD.Data)
		if cb, exists := d.ackCallbacks[dataCMD.TS]; exists {
			cb(dataCMD.TS)
			delete(d.ackCallbacks, dataCMD.TS)
		}
	case cmd.DATACMDTYPE_CACHE:
		d.ds.Cache(dataCMD.N)
		atomic.StoreInt32(&d.caching, 0)
	case cmd.DATACMDTYPE_PERSIST:
		go d.persist(dataCMD.N)
	}
}

func (d *dataNode) persist(n uint64) {
	n, err := d.ds.Persist(n)
	if err == nil {
		d.mux.Lock()
		d.ds.DeleteCache(n)
		d.mux.Unlock()
	} else {
		log.ZAPSugaredLogger().Errorf("Error raised when persisting data, err=%s.", err)
	}
	atomic.StoreInt32(&d.persisting, 0)
}

func (d *dataNode) propose(cmd cmd.DataCMD) {
	data, _ := pkg.Encode(cmd)
	d.proposeC <- data
}

func (d *dataNode) tickHeartbeat() {
	for _ = range d.hbTicker.C {
		d.heartbeat(d.nodeID, d.Metadata())
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
			err = d.recoverFromSnapshot(snapshot.Data)
			if err != nil {
				log.ZAPSugaredLogger().Fatalf("Error raised when recovering from snapshot, err=%s.", err)
				d.Stop()
				return
			}
			log.ZAPSugaredLogger().Infof("Finish loading snapshot.")
		default:
			d.mux.Lock()
			var dataCMD cmd.DataCMD
			err := pkg.Decode(*commit, &dataCMD)
			if err != nil {
				log.ZAPSugaredLogger().Errorf("Error raised when decoding commit, err=%s.", err)
				d.Stop()
				return
			}
			//log.ZAPSugaredLogger().Infof("apply cmd to DataNode : %+v", dataCMD)
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
