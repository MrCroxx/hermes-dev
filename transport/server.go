package transport

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/fwhezfwhez/tcpx"
	"mrcroxx.io/hermes/cmd"
	"mrcroxx.io/hermes/log"
	"sync"
)

const (
	RaftID              = 1
	HermesProducerCMDID = 2
	HermesProducerRSPID = 3
	HermesConsumerCMDID = 4
	HermesConsumerRSPID = 5
)

var (
	errRedirect = errors.New("NodeID not exists in this pod, redirect")
	REDIRECT    = "redirect"
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
	s.server = tcpx.NewTcpX(tcpx.ProtobufMarshaller{})
	s.server.OnConnect = s.onConnect
	s.server.OnClose = s.onClose
	s.server.AddHandler(RaftID, s.handleRaft)
	s.server.AddHandler(HermesProducerCMDID, s.handleHermesProducer)
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

	log.ZAPSugaredLogger().Debugf("got a new conn : %s .", c.ClientIP())
}

func (s *rpcServer) onClose(c *tcpx.Context) {
	log.ZAPSugaredLogger().Debugf("tcp conn closed. %+v", c)
}

func (s *rpcServer) handleRaft(c *tcpx.Context) {
	var m raftpb.Message
	_, err := c.BindWithMarshaller(&m, tcpx.JsonMarshaller{})
	//log.ZAPSugaredLogger().Debugf("%+v", m)
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

func (s *rpcServer) handleHermesProducer(c *tcpx.Context) {
	var req cmd.HermesProducerCMD
	var rsp cmd.HermesProducerRSP

	_, err := c.BindWithMarshaller(&req, tcpx.JsonMarshaller{})
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when binding message, err=%s.", err)
		rsp.Err = fmt.Sprintf("%s", err)
		err = c.ReplyWithMarshaller(tcpx.JsonMarshaller{}, HermesProducerRSPID, rsp)
		if err != nil {
			log.ZAPSugaredLogger().Errorf("Error raised when replying client, err=%s.", err)
		}
		return
	}

	rsp.NodeID, rsp.PodID = s.transport.LookUpLeader(req.ZoneID)
	if req.NodeID == 0 {
		req.NodeID = rsp.NodeID
	}
	if req.NodeID != rsp.NodeID {
		s.redirectHermesProducer(c, rsp)
		return
	}
	if !s.transport.AppendData(req.NodeID, req.TS, req.Data, func(ts int64) {
		rsp.TS = ts
		err = c.ReplyWithMarshaller(tcpx.JsonMarshaller{}, HermesProducerRSPID, rsp)
		if err != nil {
			log.ZAPSugaredLogger().Errorf("Error raised when replying client, err=%s.", err)
		}
	}) {
		s.redirectHermesProducer(c, rsp)
		return
	}
}

func (s *rpcServer) redirectHermesProducer(c *tcpx.Context, rsp cmd.HermesProducerRSP) {
	rsp.Err = REDIRECT
	err := c.ReplyWithMarshaller(tcpx.JsonMarshaller{}, HermesProducerRSPID, rsp)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when replying client, err=%s.", err)
	}
}
