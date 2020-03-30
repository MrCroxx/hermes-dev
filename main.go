package main

import (
	"flag"
	"fmt"
	"mrcroxx.io/hermes/component"
	"mrcroxx.io/hermes/config"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/pkg"
	"mrcroxx.io/hermes/unit"
	"mrcroxx.io/hermes/web"
	"sort"
	"strconv"
	"strings"
)

const banner = `
 __   __  _______  ______    __   __  _______  _______ 
|  | |  ||       ||    _ |  |  |_|  ||       ||       |
|  |_|  ||    ___||   | ||  |       ||    ___||  _____|
|       ||   |___ |   |_||_ |       ||   |___ | |_____ 
|       ||    ___||    __  ||       ||    ___||_____  |
|   _   ||   |___ |   |  | || ||_|| ||   |___  _____| |
|__| |__||_______||___|  |_||_|   |_||_______||_______|
`

func main() {
	// Sync ZAP log before terminated
	defer log.ZAPSugaredLogger().Sync()

	// Print Hermes Banner
	log.ZAPSugaredLogger().Info(banner)

	// Parse command lind args
	var c = flag.String("c", "", "path to hermes config file")
	flag.Parse()

	// Parse Hermes config file
	cfg, err := config.ParseHermesConfigFromFile(*c)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when parsing hermes config, err=%s.", err)
	}
	log.ZAPSugaredLogger().Debugf("hermes config : %+v", cfg)

	// clean .tmp file
	pkg.CleanTmp(cfg.StorageDir)

	// Initialize Pod
	ec := make(chan error)
	go func() {
		err := <-ec
		log.ZAPSugaredLogger().Fatalf("Pod err : %s.", err)
		panic(err)
	}()
	pod := component.NewPod(*cfg, ec)
	defer pod.Stop()


	if cfg.WebUIPort != 0 {
		webUI := web.NewWebUI(pod,cfg.WebUIPort)
		go webUI.Start()
		log.ZAPSugaredLogger().Infof("start metadata http server at :%d", cfg.WebUIPort)
	}

	startCMD(pod)

}

func startCMD(pod unit.Pod) {
	var cmd string
	for {
		log.ZAPSugaredLogger().Infof("Please input your cmd : ")
		fmt.Scan(&cmd)
		cmds := strings.Split(cmd, ":")
		log.ZAPSugaredLogger().Infof("%+v", cmds)
		op := cmds[0]
		switch op {
		case "all":
			if all, err := pod.All(); err == nil {
				sort.Slice(all, func(i, j int) bool {
					if all[i].ZoneID == all[j].ZoneID {
						return all[i].NodeID < all[j].NodeID
					} else {
						return all[i].ZoneID < all[j].ZoneID
					}
				})
				for _, r := range all {
					log.ZAPSugaredLogger().Infof("%+v", r)
				}

			} else {
				log.ZAPSugaredLogger().Errorf("%s", err)
			}
		case "add":
			zoneID, _ := strconv.Atoi(cmds[1])
			nodes := make(map[uint64]uint64)
			for i := 2; i < len(cmds); i += 2 {
				nid, _ := strconv.Atoi(cmds[i])
				pid, _ := strconv.Atoi(cmds[i+1])
				nodes[uint64(nid)] = uint64(pid)
			}
			if err := pod.AddRaftZone(uint64(zoneID), nodes); err == nil {
				log.ZAPSugaredLogger().Infof("ok")
			} else {
				log.ZAPSugaredLogger().Errorf("%s", err)
			}
		case "tl":
			zoneID, _ := strconv.Atoi(cmds[1])
			nodeID, _ := strconv.Atoi(cmds[2])
			if err := pod.TransferLeadership(uint64(zoneID), uint64(nodeID)); err == nil {
				log.ZAPSugaredLogger().Infof("ok")
			} else {
				log.ZAPSugaredLogger().Errorf("%s", err)
			}
		case "wk":
			nodeID, _ := strconv.Atoi(cmds[1])
			pod.WakeUpNode(uint64(nodeID))
			log.ZAPSugaredLogger().Infof("ok")
		default:
			log.ZAPSugaredLogger().Infof("none")
		}
	}
}

/*

add:10000:10001:1:10002:2:10003:3:10004:4:10005:5

add:1:11:1:12:2:13:3

add:2:22:2:23:3:24:4

add:3:33:3:34:4:35:5



go run mrcroxx.io/hermes -c f:\hermes-1.yaml

go run mrcroxx.io/hermes -c f:\hermes-2.yaml

go run mrcroxx.io/hermes -c f:\hermes-3.yaml

go run mrcroxx.io/hermes -c f:\hermes-4.yaml

go run mrcroxx.io/hermes -c f:\hermes-5.yaml

go run mrcroxx.io/hermes/producer

go run mrcroxx.io/hermes/consumer


hermes.exe -c f:\hermes-1.yaml

hermes.exe -c f:\hermes-2.yaml

hermes.exe -c f:\hermes-3.yaml

hermes.exe -c f:\hermes-4.yaml

hermes.exe -c f:\hermes-5.yaml

wget https://unpkg.com/element-ui@2.13.0/lib/theme-chalk/index.css
wget https://unpkg.com/element-ui@2.13.0/lib/theme-chalk/fonts/element-icons.woff
wget https://unpkg.com/element-ui@2.13.0/lib/theme-chalk/fonts/element-icons.ttf
wget https://unpkg.com/element-ui@2.13.0/lib/index.js


*/
