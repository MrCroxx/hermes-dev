package transport

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"mrcroxx.io/hermes/component"
	"sync"
)

// errors
func errPodNotExists(podID uint64) error { return errors.New(fmt.Sprintf("pod %d not exists", podID)) }

func errNodeNotExists(nodeID uint64) error {
	return errors.New(fmt.Sprintf("node %d not exists", nodeID))
}

func errPodExists(podID uint64) error { return errors.New(fmt.Sprintf("pod %d already exists", podID)) }

func errNodeExists(nodeID uint64) error {
	return errors.New(fmt.Sprintf("node %d already exists", nodeID))
}

func errRaftNotExists(nodeID uint64) error {
	return errors.New(fmt.Sprintf("raft %d not exists, maybe node %d is not a local node", nodeID, nodeID))
}

// transport

type Raft interface {
	Process(ctx context.Context, m raftpb.Message) error
	IsIDRemoved(id uint64) bool
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

type RPCServer interface {
	Init() error
	Close()
}

type RPCClient interface {
	Send(m raftpb.Message) error
	Close()
}

type Transport interface {
	Start() error
	Stop()
	Send(msgs []raftpb.Message)
	AddPod(podID uint64, url string) error
	RemovePod(podID uint64) error
	AddNode(podID uint64, nodeID uint64) error
	BindRaft(nodeID uint64, raft Raft)
	UnbindRaft(nodeID uint64)
	BindDataNode(nodeID uint64, d component.DataNode)
	UnbindDataNode(nodeID uint64)
	BindMetaNode(m component.MetaNode)
	RemoveNode(nodeID uint64) error
	Raft(nodeID uint64) (Raft, error)
	// implement meta node func
	LookUpLeader(zoneID uint64) uint64
	// implement data node func
	AppendData(nodeID uint64, firstIndex uint64, data []string) uint64
}

type transport struct {
	podID       uint64
	snapshotter *snap.Snapshotter
	mux         sync.RWMutex                  // RWMutex to maintain maps below
	nrafts      map[uint64]Raft               // node id -> raft (for local nodes only)
	npods       map[uint64]uint64             // node id -> pod id
	clients     map[uint64]RPCClient          // pod id -> rpc client
	nodeSets    map[uint64][]uint64           // pod id -> node ids
	ndatanodes  map[uint64]component.DataNode // node id -> DataNode
	metanode    component.MetaNode
	server      RPCServer
	url         string
	done        chan struct{}
}

func NewTransport(podID uint64, url string) Transport {
	return &transport{
		podID:    podID,
		url:      url,
		nrafts:   make(map[uint64]Raft),
		nodeSets: make(map[uint64][]uint64),
		npods:    make(map[uint64]uint64),
		clients:  make(map[uint64]RPCClient),
		done:     make(chan struct{}),
	}
}

func (t *transport) Start() error {
	t.server = NewRPCServer(t.url, t)
	return t.server.Init()
}

func (t *transport) Stop() {
	for _, c := range t.clients {
		c.Close()
	}
	close(t.done)
	t.server.Close()
}

func (t *transport) Send(msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			continue
		}
		t.mux.Lock()
		c, ok := t.clients[t.npods[m.To]]
		if ok {
			_ = c.Send(m)
		} else {
			// log.ZAPSugaredLogger().Warnf("Node %d metadata not found in pod %d, raft msg ignored.", m.To, t.podID)
		}
		t.mux.Unlock()
	}
}

func (t *transport) AddPod(podID uint64, url string) (err error) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if _, exists := t.clients[podID]; exists {
		return errPodExists(podID)
	}
	c := NewRPCClient(url)
	t.clients[podID] = c
	t.nodeSets[podID] = []uint64{}
	return
}

func (t *transport) RemovePod(podID uint64) (err error) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if c, ok := t.clients[podID]; ok {
		defer c.Close()
		delete(t.clients, podID)
		delete(t.nodeSets, podID)
	} else {
		err = errPodNotExists(podID)
	}
	return
}

func (t *transport) AddNode(podID uint64, nodeID uint64) (err error) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if _, exists := t.clients[podID]; exists {
		if _, ok := t.npods[nodeID]; !ok {
			t.npods[nodeID] = podID
			t.nodeSets[podID] = append(t.nodeSets[podID], nodeID)
		} else {
			return errNodeExists(nodeID)
		}
	} else {
		return errPodNotExists(podID)
	}
	return
}

func (t *transport) BindRaft(nodeID uint64, raft Raft) {
	t.mux.Lock()
	defer t.mux.Unlock()
	t.nrafts[nodeID] = raft
}

func (t *transport) UnbindRaft(nodeID uint64) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if _, exists := t.nrafts[nodeID]; exists {
		delete(t.nrafts, nodeID)
	}
}

func (t *transport) BindDataNode(nodeID uint64, d component.DataNode) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if d == nil {
		return
	}
	t.ndatanodes[nodeID] = d
}

func (t *transport) UnbindDataNode(nodeID uint64) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if _, exists := t.ndatanodes[nodeID]; exists {
		delete(t.ndatanodes, nodeID)
	}
}

func (t *transport) BindMetaNode(m component.MetaNode) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if m == nil {
		return
	}
	t.metanode = m
}

func (t *transport) RemoveNode(nodeID uint64) (err error) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if _, exists := t.npods[nodeID]; exists {
		delete(t.npods, nodeID)
		delete(t.nrafts, nodeID)
	} else {
		return errNodeNotExists(nodeID)
	}
	return
}

func (t *transport) Raft(nodeID uint64) (Raft, error) {
	t.mux.RLock()
	defer t.mux.RUnlock()
	r, ok := t.nrafts[nodeID]
	if ok {
		return r, nil
	}
	return nil, errRaftNotExists(nodeID)
}

// implement meta node func

func (t *transport) LookUpLeader(zoneID uint64) uint64 {
	if t.metanode == nil {
		return 0
	}
	return t.metanode.LookUpLeader(zoneID)
}

// implement data node func

func (t *transport) AppendData(nodeID uint64, firstIndex uint64, data []string) uint64 {
	if d, exists := t.ndatanodes[nodeID]; exists {
		d.Append(firstIndex, data)
		return d.Ack()
	}
	return 0
}
