package transport

import (
	"context"
	"errors"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/fwhezfwhez/tcpx"
	"mrcroxx.io/hermes/component"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/pkg"
	"sync"
)

const (
	RaftID      = 1
	HermesCMDID = 2
	HermesRSPID = 3
)

var (
	errNodeIDNotExist = errors.New("NodeID not exists, if client is not sure about node id, just set it 0")
)

type rpcServer struct {
	url       string
	transport Transport
	server    *tcpx.TcpX
	mux       sync.Mutex
}

func NewRPCServer(url string, transport Transport) RPCServer {
	return &rpcServer{
		url:       url,
		transport: transport,
	}
}

func (s *rpcServer) Init() error {
	s.server = tcpx.NewTcpX(&pkg.GOBMarshaller{})
	s.server.OnConnect = s.onConnect
	s.server.OnClose = s.onClose
	s.server.AddHandler(RaftID, s.handleRaft)
	go func() {
		err := s.server.ListenAndServe("tcp", s.url)
		if err != nil {
			log.ZAPSugaredLogger().Fatalf("Error raised when starting listen and serve, err=%s.", err)
			panic(err)
		}
	}()
	return nil
}

func (s *rpcServer) Close() {
	s.server.Stop(true)
}

func (s *rpcServer) onConnect(c *tcpx.Context) {
	log.ZAPSugaredLogger().Debugf("got a new conn.")
}

func (s *rpcServer) onClose(c *tcpx.Context) {
	log.ZAPSugaredLogger().Debugf("tcp conn closed. %+v", c)
}

func (s *rpcServer) handleRaft(c *tcpx.Context) {
	var m raftpb.Message
	_, err := c.Bind(&m)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when binding message, err=%s.", err)
		return
	}
	raft, err := s.transport.Raft(m.To)
	if err != nil {
		log.ZAPSugaredLogger().Infof("Not a message to this pod, nodeID=%d, err=%s.", m.To, err)
		return
	}
	_ = raft.Process(context.TODO(), m)
}

func (s *rpcServer) handleHermes(c *tcpx.Context) {
	var m component.HermesCMD
	var rsp component.HermesRSP

	_, err := c.Bind(&m)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when binding message, err=%s.", err)
		rsp.Err = err
		c.Reply(HermesRSPID, rsp)
		return
	}
	rsp.NodeID = s.transport.LookUpLeader(m.ZoneID)
	if m.NodeID == 0 {
		m.NodeID = rsp.NodeID
	}
	// TODO : handle data with data node
	rsp.FirstIndex = s.transport.AppendData(m.NodeID, m.FirstIndex, m.Data)
	if rsp.FirstIndex == 0 {
		rsp.Err = errNodeIDNotExist
	}
	c.Reply(HermesRSPID, rsp)
}
