package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"mrcroxx.io/hermes/cmd"
	"mrcroxx.io/hermes/log"
	"net/http"
	"time"
)

type DataReq struct {
	ZoneID     uint64   `json:"zone_id"`
	FirstIndex uint64   `json:"first_index"`
	Data       []string `json:"data"`
}

type DataRsp struct {
	ACK uint64 `json:"ack"`
}

func main() {
	http.HandleFunc("/", serverHttp)
	err := http.ListenAndServe(":15500", nil)
	if err != nil {
		panic(err)
	}
}

func serverHttp(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "PUT":
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.ZAPSugaredLogger().Error(err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		var req cmd.HermesConsumerCMD
		if err := json.Unmarshal(body, &req); err != nil {
			log.ZAPSugaredLogger().Error(err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		//zid := req.ZoneID
		//i := req.FirstIndex
		//for _, s := range req.Data {
		//	log.ZAPSugaredLogger().Infof("zone : %d, index : %d, data : %s", zid, i, s)
		//	i++
		//}
		if len(req.Data) != 0 {
			log.ZAPSugaredLogger().Infof("%d %d-%d", time.Now().Unix(), req.FirstIndex, req.FirstIndex+uint64(len(req.Data)))
		}

		rsp := cmd.HermesConsumerRSP{ACK: req.FirstIndex + uint64(len(req.Data))}
		rb, err := json.Marshal(rsp)
		if err != nil {
			log.ZAPSugaredLogger().Error(err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		if _, err := io.WriteString(w, string(rb)); err != nil {
			log.ZAPSugaredLogger().Error(err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	default:
		http.Error(w, "mothod not allowed", http.StatusMethodNotAllowed)
	}
}
