package component

import (
	"encoding/json"
	"errors"
	"fmt"
	"mrcroxx.io/hermes/config"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/store"
	"mrcroxx.io/hermes/transport"
	"mrcroxx.io/hermes/unit"
	"net/http"
	"path"
	"sync"
)

var (
	errMetaNodeNotExist = errors.New("metaNode node in this pod not exist")
)

type Metadata struct {
	Config      config.HermesConfig
	RaftRecords []store.RaftRecord
}

// pod
type pod struct {
	podID                   uint64                   // pod id
	pods                    map[uint64]string        // pod id -> url
	storageDir              string                   // path to storage
	transport               transport.Transport      // transport engine
	errC                    chan<- error             // send pod errors
	metaNode                unit.MetaNode            // mate node
	nodes                   map[uint64]unit.DataNode // node id -> data node
	triggerSnapshotEntriesN uint64                   // entries count to trigger raft snapshot
	snapshotCatchUpEntriesN uint64                   // entries count for slow follower catch up before compacting
	metaZoneOffset          uint64                   // zone id and node id offset for metaNode zone
	cfg                     config.HermesConfig      // hermes config for pod constructing
	ackCs                   map[uint64]<-chan uint64 // node id -> ack signal channel
	mux                     sync.Mutex
}

func NewPod(
	cfg config.HermesConfig,
	errC chan<- error,
// cmdC <-chan command.PodCMD,
) unit.Pod {
	// init pod struct
	p := &pod{
		cfg:                     cfg,
		podID:                   cfg.PodID,
		pods:                    cfg.Pods,
		storageDir:              cfg.StorageDir,
		triggerSnapshotEntriesN: cfg.TriggerSnapshotEntriesN,
		snapshotCatchUpEntriesN: cfg.SnapshotCatchUpEntriesN,
		metaZoneOffset:          cfg.MetaZoneOffset,
		errC:                    errC,
		nodes:                   make(map[uint64]unit.DataNode),
		transport:               transport.NewTransport(cfg.PodID, cfg.Pods[cfg.PodID]),
	}

	// init transport
	// err returns `nil` if transport is ready
	log.ZAPSugaredLogger().Debugf("starting transport ...")
	if err := p.transport.Start(); err != nil {
		p.errC <- err
		return nil
	}
	log.ZAPSugaredLogger().Debugf("transport started.")

	if cfg.WebUIPort != 0 {
		go p.startWebUI(cfg.WebUIPort)
		log.ZAPSugaredLogger().Infof("start metadata http server at :%d", cfg.WebUIPort)
	}

	p.connectCluster()
	p.startMetaNode()

	return p
}

func (p *pod) Stop() {
	p.transport.Stop()
}

func (p *pod) All() ([]store.RaftRecord, error) {
	if p.metaNode == nil {
		return nil, errMetaNodeNotExist
	}
	return p.metaNode.All(), nil
}

func (p *pod) AddRaftZone(zoneID uint64, nodes map[uint64]uint64) error {
	if p.metaNode == nil {
		return errMetaNodeNotExist
	}
	return p.metaNode.AddRaftZone(zoneID, nodes)
}

func (p *pod) TransferLeadership(zoneID uint64, nodeID uint64) error {
	if p.metaNode == nil {
		return errMetaNodeNotExist
	}
	return p.metaNode.TransferLeadership(zoneID, nodeID)
}

func (p *pod) WakeUpNode(nodeID uint64) {
	if p.metaNode == nil {
		return
	}
	p.metaNode.WakeUpNode(nodeID)
}

func (p *pod) connectCluster() {
	for podID, url := range p.pods {
		if err := p.transport.AddPod(podID, url); err != nil {
			p.errC <- err
		}
	}
}

func (p *pod) startMetaNode() {
	nodeID := p.podID + p.metaZoneOffset
	peers := make(map[uint64]uint64)
	for podID, _ := range p.pods {
		peers[podID] = podID + p.metaZoneOffset
	}

	p.metaNode = NewMetaNode(MetaNodeConfig{
		ZoneID:                  p.metaZoneOffset,
		NodeID:                  nodeID,
		PodID:                   p.podID,
		Peers:                   peers,
		Join:                    false,
		StorageDir:              path.Join(p.storageDir, fmt.Sprintf("%d", nodeID)),
		TriggerSnapshotEntriesN: p.triggerSnapshotEntriesN,
		SnapshotCatchUpEntriesN: p.snapshotCatchUpEntriesN,
		Transport:               p.transport,
		DoLeadershipTransfer:    p.doLeadershipTransfer,
		StartDataNode:           p.startDataNode,
	})
	if p.metaNode == nil {
		log.ZAPSugaredLogger().Fatalf("Failed to create metaNode node.")
		panic(nil)
	}
}

func (p *pod) startDataNode(zoneID uint64, nodeID uint64, peers map[uint64]uint64) {
	p.mux.Lock()
	defer p.mux.Unlock()
	d := NewDataNode(DataNodeConfig{
		ZoneID:                  zoneID,
		NodeID:                  nodeID,
		Peers:                   peers,
		Join:                    false,
		StorageDir:              path.Join(p.storageDir, fmt.Sprintf("%d", nodeID)),
		TriggerSnapshotEntriesN: p.triggerSnapshotEntriesN,
		SnapshotCatchUpEntriesN: p.snapshotCatchUpEntriesN,
		Transport:               p.transport,
		PushDataURL:             p.cfg.PushDataURL,
		MaxPushN:                p.cfg.MaxPushN,
		MaxCacheN:               p.cfg.MaxCacheN,
		NotifyLeaderShip:        p.metaNode.NotifyLeadership,
		Heartbeat:               p.metaNode.Heartbeat,
	})
	if d == nil {
		log.ZAPSugaredLogger().Error("Error raised when add data node")
	}
	p.nodes[nodeID] = d
}

func (p *pod) doLeadershipTransfer(podID uint64, old uint64, transferee uint64) {
	log.ZAPSugaredLogger().Debugf("do leadership transfer : pod %d old %d transferee %d.", podID, old, transferee)
	if podID != p.podID {
		return
	}
	if p.metaNode != nil && p.metaNode.NodeID() == transferee {
		p.metaNode.DoLead(old)
		return
	}
	if n, exists := p.nodes[transferee]; exists {
		n.DoLead(old)
		return
	}
}

func (p *pod) startWebUI(port uint64) {
	http.HandleFunc("/json/metadata", p.metadata)
	http.Handle("/", http.FileServer(http.Dir("./ui")))
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		log.ZAPSugaredLogger().Fatalf("Error raised when serving http, err=%s.", err)
		panic(err)
	}
}

func (p *pod) metadata(rsp http.ResponseWriter, req *http.Request) {
	rr, err := p.All()
	if err != nil {
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	metadata := Metadata{
		Config:      p.cfg,
		RaftRecords: rr,
	}
	bs, err := json.Marshal(metadata)
	if err != nil {
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	_, err = fmt.Fprintf(rsp, string(bs))
	if err != nil {
		http.Error(rsp, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}
