package client

import (
	"errors"
	"github.com/fwhezfwhez/tcpx"
	"mrcroxx.io/hermes/cmd"
	"mrcroxx.io/hermes/transport"
	"net"
	"time"
)

var (
	errConnNotEstablished = errors.New("conn has not established yet")
	errTSMissMatch        = errors.New("timestamp miss match")
	errRedirect           = errors.New("redirect")
)

type HermesClient interface {
	Send(data []string) error
}

type hermesClient struct {
	zoneID uint64
	podID  uint64
	pods   map[uint64]string
	nodeID uint64
	conn   net.Conn
	packx  *tcpx.Packx
}

type HermesClientConfig struct {
	ZoneID uint64            // zone id
	Pods   map[uint64]string // pod id -> url
}

func NewHermesClient(cfg HermesClientConfig) HermesClient {
	c := &hermesClient{
		zoneID: cfg.ZoneID,
		pods:   cfg.Pods,
		podID:  1,
		packx:  tcpx.NewPackx(tcpx.JsonMarshaller{}),
	}
	go c.tryConn()
	return c
}

func (c *hermesClient) Send(data []string) error {
	// check connection is established
	if c.conn == nil {
		return errConnNotEstablished
	}
	// encode message
	req := cmd.HermesProducerCMD{
		Type:   cmd.HERMESCMDTYPE_APPEND,
		ZoneID: c.zoneID,
		NodeID: c.nodeID,
		TS:     time.Now().Unix(),
		Data:   data,
	}
	buf, err := c.packx.Pack(transport.HermesProducerCMDID, req)
	if err != nil {
		return err
	}
	// send message

	_, err = c.conn.Write(buf)
	if err != nil {
		go c.tryConn()
		return err
	}

	//buf, err = tcpx.UnpackToBlockFromReader(c.conn)
	buf, err = tcpx.FirstBlockOf(c.conn)
	if err != nil {
		go c.tryConn()
		return err
	}

	var rsp cmd.HermesProducerRSP
	_, err = c.packx.Unpack(buf, &rsp)
	if err != nil {
		go c.tryConn()
		return err
	}
	if rsp.Err == transport.REDIRECT {
		c.podID = rsp.PodID
		c.nodeID = rsp.NodeID
		go c.tryConn()
		return errRedirect
	}
	if rsp.TS != req.TS {
		return errTSMissMatch
	}
	return err
}

func (c *hermesClient) tryConn() {
	c.conn = nil
	var err error
	for {
		c.conn, err = net.Dial("tcp", c.pods[c.podID])
		if err == nil {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}
