package component

import (
	"encoding/json"
	"errors"
	"fmt"
	"mrcroxx.io/hermes/config"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/transport"
	"net/http"
	"path"
)

var (
	errMetaNodeNotExist = errors.New("meta node in this pod not exist")
)

type Metadata struct {
	Config      config.HermesConfig
	RaftRecords []RaftRecord
}

type Pod interface {
	Stop()
	ConnectCluster()
	StartMetaNode()
	AddRaftZone(zoneID uint64, nodes map[uint64]uint64) error
	TransferLeadership(zoneID uint64, nodeID uint64) error

	All() ([]RaftRecord, error)
}

// pod
type pod struct {
	podID                   uint64              // pod id
	pods                    map[uint64]string   // pod id -> url
	storageDir              string              // path to storage
	transport               transport.Transport // transport engine
	errC                    chan<- error        // send pod errors
	meta                    MetaNode            // mate node
	nodes                   map[uint64]DataNode // node id -> data node
	triggerSnapshotEntriesN uint64              // entries count to trigger raft snapshot
	snapshotCatchUpEntriesN uint64              // entries count for slow follower catch up before compacting
	metaZoneOffset          uint64              // zone id and node id offset for meta zone
	cfg                     config.HermesConfig // hermes config for pod constructing
}

func NewPod(
	cfg config.HermesConfig,
// cmdC <-chan command.PodCMD,
) (Pod, <-chan error) {
	// init pod struct
	ec := make(chan error)
	p := &pod{
		cfg:                     cfg,
		podID:                   cfg.PodID,
		pods:                    cfg.Pods,
		storageDir:              cfg.StorageDir,
		triggerSnapshotEntriesN: cfg.TriggerSnapshotEntriesN,
		snapshotCatchUpEntriesN: cfg.SnapshotCatchUpEntriesN,
		metaZoneOffset:          cfg.MetaZoneOffset,
		errC:                    ec,
		nodes:                   make(map[uint64]DataNode),
		transport:               transport.NewTransport(cfg.PodID, cfg.Pods[cfg.PodID]),
	}

	// init transport
	// err returns `nil` if transport is ready
	log.ZAPSugaredLogger().Debugf("starting transport ...")
	if err := p.transport.Start(); err != nil {
		p.errC <- err
		return nil, ec
	}
	log.ZAPSugaredLogger().Debugf("transport started.")

	if cfg.WebUIPort != 0 {
		go p.startWebUI(cfg.WebUIPort)
		log.ZAPSugaredLogger().Infof("start metadata http server at :%d", cfg.WebUIPort)
	}

	return p, ec
}

func (p *pod) Stop() {
	p.transport.Stop()
}

func (p *pod) ConnectCluster() {
	for podID, url := range p.pods {
		if err := p.transport.AddPod(podID, url); err != nil {
			p.errC <- err
		}
	}
}

func (p *pod) StartMetaNode() {
	nodeID := p.podID + p.metaZoneOffset
	peers := make(map[uint64]uint64)
	for podID, _ := range p.pods {
		peers[podID] = podID + p.metaZoneOffset
	}

	p.meta = NewMetaNode(MetaNodeConfig{
		ZoneID:                  p.metaZoneOffset,
		NodeID:                  nodeID,
		PodID:                   p.podID,
		Peers:                   peers,
		Join:                    false,
		StorageDir:              path.Join(p.storageDir, fmt.Sprintf("%d", nodeID)),
		TriggerSnapshotEntriesN: p.triggerSnapshotEntriesN,
		SnapshotCatchUpEntriesN: p.snapshotCatchUpEntriesN,
		Transport:               p.transport,
		DoLeadershipTransfer:    p.DoLeadershipTransfer,
		StartDataNode:           p.StartDataNode,
	})
	if p.meta == nil {
		log.ZAPSugaredLogger().Fatalf("Failed to create meta node.")
		panic(nil)
	}
}

func (p *pod) StartDataNode(zoneID uint64, nodeID uint64, peers map[uint64]uint64) {
	d := NewDataNode(DataNodeConfig{
		ZoneID:                  zoneID,
		NodeID:                  nodeID,
		Peers:                   peers,
		Join:                    false,
		StorageDir:              path.Join(p.storageDir, fmt.Sprintf("%d", nodeID)),
		TriggerSnapshotEntriesN: p.triggerSnapshotEntriesN,
		SnapshotCatchUpEntriesN: p.snapshotCatchUpEntriesN,
		Transport:               p.transport,
		NotifyLeaderShip:        p.meta.NotifyLeadership,
		Heartbeat:               p.meta.Heartbeat,
	})
	if d == nil {
		log.ZAPSugaredLogger().Error("Error raised when add data node")
	}
	p.nodes[nodeID] = d
}

func (p *pod) All() ([]RaftRecord, error) {
	if p.meta == nil {
		return nil, errMetaNodeNotExist
	}
	return p.meta.All(), nil
}

func (p *pod) AddRaftZone(zoneID uint64, nodes map[uint64]uint64) error {
	if p.meta == nil {
		return errMetaNodeNotExist
	}
	return p.meta.AddRaftZone(zoneID, nodes)
}

func (p *pod) TransferLeadership(zoneID uint64, nodeID uint64) error {
	if p.meta == nil {
		return errMetaNodeNotExist
	}
	return p.meta.TransferLeadership(zoneID, nodeID)
}

func (p *pod) DoLeadershipTransfer(podID uint64, old uint64, transferee uint64) {
	log.ZAPSugaredLogger().Debugf("do leadership transfer : pod %d old %d transferee %d.", podID, old, transferee)
	if podID != p.podID {
		return
	}
	if p.meta != nil && p.meta.NodeID() == transferee {
		p.meta.DoLead(old)
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
