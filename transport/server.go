package transport

import (
	"context"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/fwhezfwhez/tcpx"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/pkg"
)

const (
	RaftHandler = 1
)

type rpcServer struct {
	url       string
	transport Transport
	server    *tcpx.TcpX
}

func NewRPCServer(url string, transport Transport) RPCServer {
	return &rpcServer{url: url, transport: transport}
}

func (s *rpcServer) Init() error {
	s.server = tcpx.NewTcpX(&pkg.GOBMarshaller{})
	s.server.OnConnect = s.onConnect
	s.server.OnClose = func(ctx *tcpx.Context) { s.onClose(ctx) }
	s.server.AddHandler(RaftHandler, s.handleRaft)
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
	log.ZAPSugaredLogger().Debugf("tcp conn closed.")
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
