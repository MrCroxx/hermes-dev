package transport

import (
	"errors"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/fwhezfwhez/tcpx"
	"net"
	"time"
)

var (
	errFailToInit         = errors.New("out of retry times")
	errConnNotEstablished = errors.New("conn has not established yet")
)

type rpcClient struct {
	url   string
	conn  net.Conn
	packx *tcpx.Packx
}

func NewRPCClient(url string) RPCClient {
	c := &rpcClient{url: url, packx: tcpx.NewPackx(tcpx.JsonMarshaller{})}
	go c.tryConn()
	return c
}

func (c *rpcClient) Send(m raftpb.Message) error {
	// check connection is established
	if c.conn == nil {
		return errConnNotEstablished
	}
	// encode message
	//buf, err := tcpx.PackWithMarshaller(tcpx.Message{
	//	MessageID: RaftID,
	//	Body:      m,
	//}, &tcpx.JsonMarshaller{})
	buf, err := c.packx.Pack(RaftID, m)
	if err != nil {
		return err
	}
	// send message
	_, err = c.conn.Write(buf)
	if err != nil {
		go c.tryConn()
	}
	return err
}

func (c *rpcClient) Close() {
	c.conn.Close()
	c.conn = nil
}

func (c *rpcClient) tryConn() {
	c.conn = nil
	var err error
	for {
		c.conn, err = net.Dial("tcp", c.url)
		if err == nil {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}
