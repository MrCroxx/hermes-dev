package main

import (
	"mrcroxx.io/hermes/client/client"
	"mrcroxx.io/hermes/cmd"
	"mrcroxx.io/hermes/log"
	"time"
)

func main() {
	c := client.NewHermesClient(client.HermesClientConfig{
		ZoneID: 1,
		Pods: map[uint64]string{
			1: "127.0.0.1:14401",
			2: "127.0.0.1:14402",
			3: "127.0.0.1:14403",
			4: "127.0.0.1:14404",
			5: "127.0.0.1:14405",
		},
	})
	time.Sleep(time.Second * 1)
	err := c.Send(cmd.HermesCMD{
		Type:       cmd.HERMESCMDTYPE_APPEND,
		ZoneID:     1,
		NodeID:     0,
		FirstIndex: 0,
		Data:       []string{},
	})
	if err != nil {
		log.ZAPSugaredLogger().Error("%s", err)
	}
	log.ZAPSugaredLogger().Debugf("finish")
}
