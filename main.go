package main

import (
	"flag"
	"fmt"
	"mrcroxx.io/hermes/component"
	"mrcroxx.io/hermes/config"
	"mrcroxx.io/hermes/log"
	"sort"
	"strconv"
	"strings"
)

func showRT(rt *component.RaftTable) {
	log.ZAPSugaredLogger().Infof("%+v", rt.All())
}

/*

add:10000:10001:1:10002:2:10003:3:10004:4:10005:5

add:1:11:1:12:2:13:3

add:2:21:1:22:2:23:3:24:4:25:5

add:3:31:1:32:2:33:3

add:4:41:1:42:2:43:3

add:5:51:1:52:2:53:3

add:6:61:1:62:2:63:3

add:7:71:1:72:2:73:3

add:8:81:1:82:2:83:3

add:9:91:1:92:2:93:3

add:10:101:1:102:2:103:3






./raftexample --id 1 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 12380
./raftexample --id 2 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 22380
./raftexample --id 3 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 32380

go run mrcroxx.io/hermes -c f:\hermes-1.yaml
go run mrcroxx.io/hermes -c f:\hermes-2.yaml
go run mrcroxx.io/hermes -c f:\hermes-3.yaml
go run mrcroxx.io/hermes -c f:\hermes-4.yaml
go run mrcroxx.io/hermes -c f:\hermes-5.yaml

*/

func main() {
	defer log.ZAPSugaredLogger().Sync()

	// Parse commend arguments

	var c = flag.String("c", "", "path to hermes config file")
	flag.Parse()

	cfg, err := config.ParseHermesConfigFromFile(*c)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when parsing hermes config, err=%s.", err)
	}
	log.ZAPSugaredLogger().Debugf("hermes config : %+v", cfg)

	// t := transport.NewTransport()

	// TODO : Create pod
	pod, podErrC := component.NewPod(*cfg)
	//defer pod.Stop()

	go func() {
		err := <-podErrC
		log.ZAPSugaredLogger().Fatalf("Pod err : %s.", err)
	}()

	var cmd string

	log.ZAPSugaredLogger().Debugf("Pod rpc server started, input anything to trigger connections.")
	// fmt.Scan(&cmd)
	log.ZAPSugaredLogger().Debugf("Pod connecting to other pods.")
	pod.ConnectCluster()

	log.ZAPSugaredLogger().Debugf("Pod connected, input anything to trigger starting meta node.")
	// fmt.Scan(&cmd)
	log.ZAPSugaredLogger().Debugf("Pod starting meta node.")
	pod.StartMetaNode()

	for {
		log.ZAPSugaredLogger().Debugf("Please input your cmd : ")
		fmt.Scan(&cmd)
		cmds := strings.Split(cmd, ":")
		log.ZAPSugaredLogger().Debugf("%+v", cmds)
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
				log.ZAPSugaredLogger().Debugf("ok")
			} else {
				log.ZAPSugaredLogger().Errorf("%s", err)
			}
		case "tl":
			zoneID, _ := strconv.Atoi(cmds[1])
			nodeID, _ := strconv.Atoi(cmds[2])
			if err := pod.TransferLeadership(uint64(zoneID), uint64(nodeID)); err == nil {
				log.ZAPSugaredLogger().Debugf("ok")
			} else {
				log.ZAPSugaredLogger().Errorf("%s", err)
			}
		default:
			log.ZAPSugaredLogger().Debugf("none")
		}
	}

}

// TODO : Raft Table Test
//rt := component.NewRaftTable()
//
//showRT(rt)
//
//rt.Insert([]component.RaftRecord{{ZoneID: uint64(1), NodeID: uint64(1), PodID: uint64(1), IsLeader: true}})
//
//showRT(rt)
//
//rt.Insert([]component.RaftRecord{
//	{ZoneID: uint64(2), NodeID: uint64(2), PodID: uint64(2), IsLeader: true},
//	{ZoneID: uint64(3), NodeID: uint64(3), PodID: uint64(3), IsLeader: true},
//})
//
//showRT(rt)
//
//rt.Update(
//	func(rr component.RaftRecord) bool {
//		if rr.ZoneID == uint64(3) {
//			return true
//		}
//		return false
//	},
//	func(rr *component.RaftRecord) {
//		rr.ZoneID = uint64(4)
//		rr.NodeID = uint64(4)
//		rr.PodID = uint64(4)
//		rr.IsLeader = false
//	},
//)
//
//showRT(rt)
//
//rt.Delete(func(rr component.RaftRecord) bool {
//	if rr.ZoneID == uint64(2) {
//		return true
//	}
//	return false
//})
//
//showRT(rt)
//
//rt.Query(func(rr component.RaftRecord) bool {
//	if rr.NodeID == uint64(1) {
//		return true
//	}
//	return false
//})[0].NodeID = uint64(20000)
//
//showRT(rt)
//
//rt.All()[0].ZoneID = uint64(10000)
//
//showRT(rt)

// TODO : channel block test
//c := make(chan int)
//for i := 0; i < 100; i++ {
//	t:=i
//	go func() { c <- t }()
//}
//for i := range c {
//	time.Sleep(time.Millisecond * 200)
//	log.ZAPSugaredLogger().Infof("%d", i)
//}

//cmdC := make(chan command.PodCMD)
//pod, errC := component.NewPod(cfg.PodID, cfg.Pods, cmdC)
//for {
//	select {
//	case err := <-errC:
//		log.ZAPSugaredLogger().Errorf("Pod error : %s.", err)
//		pod.Stop()
//	}
//}

// TODO : transport test
//t := transport.NewTransport(cfg.PodID, cfg.Pods[cfg.PodID])
//if err := <-t.Start(); err != nil {
//	log.ZAPSugaredLogger().Fatalf("Error raised when starting transport, err=%s", err)
//}
//t.AddPod(cfg.PodID, cfg.Pods[cfg.PodID])
//t.BindRaft(uint64(1), nil)
//
//t.Send([]raftpb.Message{raftpb.Message{Type: raftpb.MsgBeat, To: uint64(1)}})
// TODO : Close channel test
//c := make(chan struct{})
//t := time.NewTimer(time.Second * 3)
//tt := time.NewTicker(time.Millisecond * 300)
//
//log.ZAPSugaredLogger().Debugf("start")
//for {
//	select {
//	case <-c:
//		log.ZAPSugaredLogger().Debugf("finish")
//		return
//	case <-t.C:
//		log.ZAPSugaredLogger().Debugf("kill")
//		close(c)
//	case <-tt.C:
//		log.ZAPSugaredLogger().Debugf("tick")
//	}
//}

// TODO : RPC test
//log.ZAPSugaredLogger().Infof("Hello Hermes!")
//raftServer := transport.NewServer()
//go func() {
//	if err := raftServer.Run(); err != nil {
//		log.ZAPSugaredLogger().Errorf("error raised when initializing raft rpc server, err=%s", err)
//	}
//}()
//time.Sleep(time.Second * 1)
//raftClient := transport.NewClient()
//msg := pb.Message{Type: pb.MsgHup}
//var wg sync.WaitGroup
//wg.Add(10000)
//for i := 0; i < 10000; i++ {
//	go func() {
//		if rsp, err := raftClient.Send(&msg); err != nil {
//			log.ZAPSugaredLogger().Errorf("error raised when processing RPC, err=%s", err)
//		} else {
//			log.ZAPSugaredLogger().Debugf("rsp : %+v", rsp)
//		}
//		wg.Done()
//	}()
//}
//wg.Wait()
//log.ZAPSugaredLogger().Debugf("finish")
