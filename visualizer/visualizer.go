package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"log"
	"strconv"
	"sync"

	//"github.com/gorilla/websocket"
	"github.com/robertli8629/go-vr/kv"
	"github.com/robertli8629/go-vr/server"
	"github.com/robertli8629/go-vr/vr"
)

const NumInstances = 5
const ServerPortStart = 10340
const PeerPortStart = 11340
const CentralServerForwardPort = 12340
const CentralServerHomePort = 12341

var lock sync.RWMutex
var actualPeerUris map[int64]string

func main() {
	ids := make([]int64, NumInstances)
	serverPorts := make([]int, NumInstances)
	peerPorts := make([]int, NumInstances)
	proxyPeerUris := map[int64]string{}
	actualPeerUris = map[int64]string{}
	for i := 0; i < NumInstances; i++ {
		ids[i] = int64(i)
		serverPorts[i] = ServerPortStart + i
		peerPorts[i] = PeerPortStart + i
		actualPeerUris[ids[i]] = "http://127.0.0.1:" + strconv.Itoa(peerPorts[i])
		proxyPeerUris[ids[i]] = "http://127.0.0.1:" + strconv.Itoa(CentralServerForwardPort)
	}

	loggers := make([]*vr.JsonLogger, NumInstances)
	messengers := make([]*vr.JsonMessenger, NumInstances)
	replications := make([]*vr.VR, NumInstances)
	stores := make([]*kv.KVStore, NumInstances)
	servers := make([]*server.Server, NumInstances)
	for i := 0; i < NumInstances; i++ {
		filename := "logv" + strconv.Itoa(i)
		loggers[i] = vr.NewJsonLogger(&filename, false)
		messengers[i] = vr.NewJsonMessenger(":" + strconv.Itoa(peerPorts[i]), proxyPeerUris)
		replications[i] = vr.NewVR(i == 0, int64(i), messengers[i], loggers[i], ids)
		stores[i] = kv.NewKVStore(int64(i), replications[i])
		servers[i] = server.NewServer(strconv.Itoa(serverPorts[i]), stores[i])
	}

	http.HandleFunc("/", forwardRequest)
	//http.HandleFunc("/ws", serveWs)
	if err := http.ListenAndServe(":" + strconv.Itoa(CentralServerForwardPort), nil); err != nil {
		log.Fatal(err)
	}

	// Do not exit
	<-make(chan interface{})
}

type Path struct {
	From int64
	To   int64
}

func forwardRequest(w http.ResponseWriter, r *http.Request) {
	lock.Lock()
	defer lock.Unlock()

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body %v, err = %v", b, err)
	}

	var path Path
	err = json.Unmarshal(b, &path)
	if err != nil {
		log.Printf("Error parsing %v, err = %v", b, err)
	}

	client := &http.Client{}
	req, err := http.NewRequest(r.Method, actualPeerUris[path.To] + r.URL.Path, bytes.NewBuffer(b))
	if err != nil {
		log.Printf("Error generating request %v, err = %v", req, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		log.Printf("Error sending request %v, err = %v", req, err)
	}

	w.WriteHeader(http.StatusOK)
}
