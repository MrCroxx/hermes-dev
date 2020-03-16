package transport

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"sync"
)

// errors
func errPodNotExists(podID uint64) error { return errors.New(fmt.Sprintf("pod %d not exists")) }

func errNodeNotExists(nodeID uint64) error { return errors.New(fmt.Sprintf("node %d not exists")) }

func errPodExists(podID uint64) error { return errors.New(fmt.Sprintf("pod %d already exists")) }

func errNodeExists(nodeID uint64) error { return errors.New(fmt.Sprintf("node %d already exists")) }

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
	BindRaft(nodeID uint64, raft Raft) error
	RemoveNode(nodeID uint64) error
	Raft(nodeID uint64) (Raft, error)
	// SendSnapshot(m snap.Message)
}

type transport struct {
	podID       uint64
	snapshotter *snap.Snapshotter
	mux         sync.RWMutex         // RWMutex to maintain maps below
	nrafts      map[uint64]Raft      // node id -> raft (for local nodes only)
	npods       map[uint64]uint64    // node id -> pod id
	clients     map[uint64]RPCClient // pod id -> rpc client
	nodeSets    map[uint64][]uint64  // pod id -> node ids
	server      RPCServer
	url         string
	done        chan struct{}
	// nclients    map[uint64]RPCClient // node id -> rpc client

}

func NewTransport(podID uint64, url string) Transport {
	return &transport{
		podID:    podID,
		url:      url,
		nrafts:   make(map[uint64]Raft),
		nodeSets: make(map[uint64][]uint64),
		npods:   make(map[uint64]uint64),
		clients: make(map[uint64]RPCClient),
		done:    make(chan struct{}),
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
	// 	log.ZAPSugaredLogger().Debugf("npods : %+v", t.npods)
	for _, m := range msgs {
		if m.To == 0 {
			continue
		}
		t.mux.Lock()
		// log.ZAPSugaredLogger().Debugf("to : %d", m.To)
		c, ok := t.clients[t.npods[m.To]]
		if ok {
			// send message
			_ = c.Send(m)
			//err := c.Send(m)
			// report unreachable
			//if err != nil {
			//	r, err := t.Raft(m.From)
			//	if err != nil {
			//		log.ZAPSugaredLogger().Errorf("Error raised when reporting unreachable.")
			//	} else {
			//		r.ReportUnreachable(m.To)
			//	}
			//
			//}
		} else {
			// log.ZAPSugaredLogger().Warnf("Node %d metadata not found in pod %d, raft msg ignored.", m.To, t.podID)
		}
		t.mux.Unlock()
	}
}

func (t *transport) AddPod(podID uint64, url string) (err error) {
	if _, exists := t.clients[podID]; exists {
		return errPodExists(podID)
	}
	c := NewRPCClient(url)
	t.mux.Lock()
	t.clients[podID] = c
	t.nodeSets[podID] = []uint64{}
	t.mux.Unlock()
	return
}

func (t *transport) RemovePod(podID uint64) (err error) {
	t.mux.Lock()
	if c, ok := t.clients[podID]; ok {
		defer c.Close()
		delete(t.clients, podID)
		delete(t.nodeSets, podID)
	} else {
		err = errPodNotExists(podID)
	}
	t.mux.Unlock()
	return
}

func (t *transport) AddNode(podID uint64, nodeID uint64) (err error) {
	if _, exists := t.clients[podID]; exists {
		if _, ok := t.npods[nodeID]; !ok {
			t.mux.Lock()
			t.npods[nodeID] = podID
			t.nodeSets[podID] = append(t.nodeSets[podID], nodeID)
			t.mux.Unlock()
		} else {
			return errNodeExists(nodeID)
		}
	} else {
		return errPodNotExists(podID)
	}
	return
}

func (t *transport) BindRaft(nodeID uint64, raft Raft) (err error) {
	if _, exists := t.clients[t.podID]; exists {
		t.mux.Lock()
		t.nrafts[nodeID] = raft
		t.mux.Unlock()
	} else {
		return errNodeNotExists(nodeID)
	}
	return
}

func (t *transport) RemoveNode(nodeID uint64) (err error) {
	if _, exists := t.npods[nodeID]; exists {
		t.mux.Lock()
		delete(t.npods, nodeID)
		delete(t.nrafts, nodeID)
		t.mux.Unlock()
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
