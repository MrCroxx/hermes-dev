package main

import (
	"mrcroxx.io/hermes/producer/client"
	"mrcroxx.io/hermes/log"
	"time"
)

func useClient() {
	c := client.NewHermesClient(client.HermesClientConfig{
		ZoneID: 2,
		Pods: map[uint64]string{
			1: "127.0.0.1:14401",
			2: "127.0.0.1:14402",
			3: "127.0.0.1:14403",
			4: "127.0.0.1:14404",
			5: "127.0.0.1:14405",
		},
	})

	i := uint64(0)
	for {
		time.Sleep(time.Millisecond * 10)
		err := c.Send([]string{"窝窝头,一块钱四个,嘿嘿!"})
		i++
		log.ZAPSugaredLogger().Infof("%d", i)
		if err != nil {
			log.ZAPSugaredLogger().Error(err)
		}
	}
}

func useFlight() {
	commitC := make(chan string)
	errC := client.NewFlight(client.FlightConfig{
		ZoneID: 1,
		Pods: map[uint64]string{
			1: "127.0.0.1:14401",
			2: "127.0.0.1:14402",
			3: "127.0.0.1:14403",
			4: "127.0.0.1:14404",
			5: "127.0.0.1:14405",
		},
		CommitC: commitC,
	})

	go func() {
		for err := range errC {
			log.ZAPSugaredLogger().Error(err)
		}
	}()
	i := uint64(0)
	for {
		time.Sleep(time.Millisecond * 1)
		commitC <- "窝窝头,一块钱四个,嘿嘿!"
		i++
		log.ZAPSugaredLogger().Infof("%d", i)
	}
}

func main() {
	useFlight()
	//useClient()
}
