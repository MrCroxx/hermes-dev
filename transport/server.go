package transport

import (
	"context"
	"errors"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/fwhezfwhez/tcpx"
	"mrcroxx.io/hermes/cmd"
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
	errRedirect = errors.New("NodeID not exists in this pod, redirect")
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
	s.server.AddHandler(HermesCMDID, s.handleHermes)
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
	var m cmd.HermesCMD
	var rsp cmd.HermesRSP

	_, err := c.Bind(&m)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when binding message, err=%s.", err)
		rsp.Err = err
		err = c.ReplyWithMarshaller(&pkg.GOBMarshaller{}, HermesRSPID, rsp)
		if err != nil {
			log.ZAPSugaredLogger().Errorf("Error raised when replying client, err=%s.", err)
		}
		return
	}

	log.ZAPSugaredLogger().Debugf("%+v", m)
	rsp.NodeID, rsp.PodID = s.transport.LookUpLeader(m.ZoneID)
	if m.NodeID == 0 {
		m.NodeID = rsp.NodeID
	}
	rsp.FirstIndex = s.transport.AppendData(m.NodeID, m.FirstIndex, m.Data)
	if rsp.FirstIndex == 0 {
		rsp.Err = errRedirect
	}
	log.ZAPSugaredLogger().Debugf("%+v", rsp)
	buf, err := tcpx.PackWithMarshaller(tcpx.Message{
		MessageID: RaftID,
		Header:    nil,
		Body:      rsp,
	}, &pkg.GOBMarshaller{})
	_, err = c.Conn.Write(buf)

	//c.ProtoBuf()
	err = c.Reply()
	//err = c.ReplyWithMarshaller(&pkg.GOBMarshaller{}, HermesRSPID, rsp)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when replying client, err=%s.", err)
	}
	log.ZAPSugaredLogger().Debugf("finish response")
}
