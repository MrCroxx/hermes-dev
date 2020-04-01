package client

import (
	"errors"
	"mrcroxx.io/hermes/log"
	"sync"
	"time"
)

const waterline = 10000

var (
	errWaterlineOverflow = errors.New("waterline overflow")
)

type flight struct {
	commitC <-chan string
	errC    chan<- error
	client  ProducerClient
	cache   []string
	done    chan struct{}
	mux     sync.Mutex
}

type FlightConfig struct {
	ZoneID  uint64
	Pods    map[uint64]string
	CommitC <-chan string
}

func NewFlight(cfg FlightConfig) <-chan error {
	eC := make(chan error)
	f := &flight{
		commitC: cfg.CommitC,
		errC:    eC,
		cache:   make([]string, 0),
		done:    make(chan struct{}),
		client: NewProducerClient(ProducerClientConfig{
			ZoneID: cfg.ZoneID,
			Pods:   cfg.Pods,
		}),
	}
	go f.push()
	go f.start()
	return eC
}

func (f *flight) start() {
	for {
		select {
		case s, ok := <-f.commitC:
			if !ok {
				close(f.done)
				continue
			}
			f.mux.Lock()
			f.cache = append(f.cache, s)
			f.mux.Unlock()
		case <-f.done:
			close(f.errC)
			break
		}
	}
}

func (f *flight) push() {
	for {
		select {
		case <-f.done:
			break
		default:
			n := len(f.cache)
			err := f.client.Send(f.cache[:n])
			if err == nil {
				f.mux.Lock()
				f.cache = f.cache[n:]
				f.mux.Unlock()
				continue
			}
			log.ZAPSugaredLogger().Error(err)
		}
	}
}

func (f *flight) watch() {
	for {

		time.Sleep(time.Second * 3)

		select {
		case <-f.done:
			break
		default:
			if len(f.cache) > waterline {
				f.errC <- errWaterlineOverflow
			}
		}
	}
}
