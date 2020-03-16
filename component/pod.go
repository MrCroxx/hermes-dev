package component

import (
	"errors"
	"github.com/coreos/etcd/raft/raftpb"
	"mrcroxx.io/hermes/config"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/transport"
)

var (
	errMetaNodeNotExist        = errors.New("meta node in this pod not exist")
	metaNodeIDOffset    uint64 = 10000
)

// raft info struct
type RaftInfo struct {
	ZoneID uint64
	PodID  uint64
	NodeID uint64
}

type RaftInfoTable []RaftInfo

func (rt RaftInfoTable) Filter(f func(ri RaftInfo) bool) []RaftInfo {
	result := []RaftInfo{}
	for _, ri := range rt {
		if f(ri) {
			result = append(result, ri)
		}
	}
	return result
}

type Pod interface {
	Stop()
	ConnectCluster()
	StartMetaNode()
	AddRaftZone(zoneID uint64, nodes map[uint64]uint64) error
	TransferLeadership(zoneID uint64, nodeID uint64) error

	All() ([]RaftRecord, error)
}

type chans struct {
	proposeC    chan<- []byte
	confChangeC chan<- raftpb.ConfChange
	errorC      <-chan error
}

// pod
type pod struct {
	podID                   uint64              // pod id
	pods                    map[uint64]string   // pod id -> url
	storageDir              string              // path to storage
	transport               transport.Transport // transport engine
	errC                    chan<- error        // send pod errors
	meta                    MetaNode            // mate node chans
	nodes                   map[uint64]*chans   // node id -> node chan
	triggerSnapshotEntriesN uint64              // entries count to trigger raft snapshot
	snapshotCatchUpEntriesN uint64              // entries count for slow follower catch up before compacting
	// cmdC      <-chan command.PodCMD // receive user cmds
}

func NewPod(
	cfg config.HermesConfig,
// cmdC <-chan command.PodCMD,
) (Pod, <-chan error) {
	// init pod struct
	ec := make(chan error)
	newPod := &pod{
		podID:                   cfg.PodID,
		pods:                    cfg.Pods,
		storageDir:              cfg.StorageDir,
		triggerSnapshotEntriesN: cfg.Meta.TriggerSnapshotEntriesN,
		snapshotCatchUpEntriesN: cfg.Meta.SnapshotCatchUpEntriesN,
		// cmdC:      cmdC,
		errC:      ec,
		nodes:     make(map[uint64]*chans),
		transport: transport.NewTransport(cfg.PodID, cfg.Pods[cfg.PodID]),
	}

	// init transport
	// err returns `nil` if transport is ready
	log.ZAPSugaredLogger().Debugf("starting transport ...")
	if err := newPod.transport.Start(); err != nil {
		newPod.errC <- err
		return nil, ec
	}
	log.ZAPSugaredLogger().Debugf("transport started.")
	return newPod, ec
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
	nodeID := p.podID + metaNodeIDOffset
	peers := make(map[uint64]uint64)
	for podID, _ := range p.pods {
		peers[podID] = podID + metaNodeIDOffset
	}
	//p.meta = NewMetaNode(p.podID, nodeID, peers, p.transport, p.storageDir, p.DoLeadershipTransfer)
	// TODO : cfg
	p.meta = NewMetaNode(MetaNodeConfig{
		ZoneID:                  metaNodeIDOffset,
		NodeID:                  nodeID,
		Pods:                    peers,
		Join:                    false,
		StorageDir:              p.storageDir,
		TriggerSnapshotEntriesN: p.triggerSnapshotEntriesN,
		SnapshotCatchUpEntriesN: p.snapshotCatchUpEntriesN,
		Transport:               p.transport,
		DoLeadershipTransfer:    p.DoLeadershipTransfer,
	})
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
	}
	// TODO : if not meta node
}
