package client

import (
	"errors"
	"github.com/fwhezfwhez/tcpx"
	"mrcroxx.io/hermes/cmd"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/pkg"
	"mrcroxx.io/hermes/transport"
	"net"
	"time"
)

var (
	errConnNotEstablished = errors.New("conn has not established yet")
)

type HermesClient interface {
	Send(hermesCMD cmd.HermesCMD) error
}

type hermesClient struct {
	zoneID uint64
	podID  uint64
	pods   map[uint64]string
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
		packx:  tcpx.NewPackx(&pkg.GOBMarshaller{}),
	}
	go c.tryConn()
	return c
}

func (c *hermesClient) Send(hermesCMD cmd.HermesCMD) error {
	// check connection is established
	if c.conn == nil {
		return errConnNotEstablished
	}
	// encode message
	log.ZAPSugaredLogger().Debugf("marshall request")
	buf, err := tcpx.PackWithMarshaller(tcpx.Message{
		MessageID: transport.HermesCMDID,
		Header:    nil,
		Body:      hermesCMD,
	}, &pkg.GOBMarshaller{})
	if err != nil {
		return err
	}
	// send message
	log.ZAPSugaredLogger().Debugf("send request")

	_, err = c.conn.Write(buf)
	if err != nil {
		go c.tryConn()
		return err
	}
	log.ZAPSugaredLogger().Debugf("read response")

	//buf, err = tcpx.UnpackToBlockFromReader(c.conn)
	buf, err = tcpx.FirstBlockOf(c.conn)
	if err != nil {
		go c.tryConn()
		return err
	}
	log.ZAPSugaredLogger().Debugf("unmarshall response")

	var rsp cmd.HermesRSP
	msg, err := c.packx.Unpack(buf, &rsp)
	if err != nil {
		go c.tryConn()
		return err
	}
	log.ZAPSugaredLogger().Debugf("msg : %+v", msg)
	log.ZAPSugaredLogger().Debugf("rsp : %+v", rsp)
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
