package client

import (
	"errors"
	"github.com/fwhezfwhez/tcpx"
	"mrcroxx.io/hermes/cmd"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/transport"
	"net"
	"time"
)

var (
	errConnNotEstablished = errors.New("conn has not established yet")
	errTSMissMatch        = errors.New("timestamp miss match")
	errRedirect           = errors.New("redirect")
	errSkip               = errors.New("skip")
	errLostIndex          = errors.New("lost index")
)

type ProducerClient interface {
	Send(data []string) error
}

type producerClient struct {
	zoneID uint64
	podID  uint64
	index  uint64
	pods   map[uint64]string
	conn   net.Conn
	packx  *tcpx.Packx
}

type ProducerClientConfig struct {
	ZoneID uint64            // zone id
	Pods   map[uint64]string // pod id -> url
}

func NewProducerClient(cfg ProducerClientConfig) ProducerClient {
	c := &producerClient{
		zoneID: cfg.ZoneID,
		pods:   cfg.Pods,
		podID:  1,
		packx:  tcpx.NewPackx(tcpx.JsonMarshaller{}),
	}
	go c.tryConn()
	return c
}

func (c *producerClient) Send(data []string) error {
	log.ZAPSugaredLogger().Debugf("push data %d ~ %d", c.index, c.index+uint64(len(data)-1))

	// check connection is established
	if c.conn == nil {
		return errConnNotEstablished
	}
	// encode message
	req := cmd.HermesProducerCMD{
		ZoneID: c.zoneID,
		TS:     time.Now().Unix(),
		Index:  c.index,
	}
	skip := true
	if c.index > 0 {
		req.Data = data
		skip = false
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
	//log.ZAPSugaredLogger().Debugf("%+v", rsp)
	if rsp.Err == transport.REDIRECT {
		c.podID = rsp.PodID
		go c.tryConn()
		return errRedirect
	}
	c.index = rsp.Index
	if skip {
		return errSkip
	}
	if rsp.TS != req.TS {
		return errTSMissMatch
	}
	if rsp.Err == transport.LOSTINDEX {
		return errLostIndex
	}
	return nil
}

func (c *producerClient) tryConn() {
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
