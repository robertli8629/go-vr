package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/gorilla/websocket"
	"github.com/robertli8629/go-vr/kv"
	"github.com/robertli8629/go-vr/server"
	"github.com/robertli8629/go-vr/vr"
)

const NumInstances = 5
const ServerPortStart = 10340
const PeerPortStart = 11340
const CentralForwarderPort = 12340
const CentralWebServerPort = 12341
const CentralWebSocketPort = 12342

// Time allowed to write the file to the client.
const WriteWait = 10 * time.Second

// Time allowed to read the next pong message from the client.
const PongWait = 60 * time.Second

// Send pings to client with this period. Must be less than pongWait.
const PingPeriod = (PongWait * 9) / 10

var lock sync.RWMutex
var actualPeerUris map[int64]string
var messageChan chan *ForwardMessage

var homeTempl = template.Must(template.ParseFiles("template/index.template"))

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func main() {
	messageChan = make(chan *ForwardMessage, 1000)

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
		proxyPeerUris[ids[i]] = "http://127.0.0.1:" + strconv.Itoa(CentralForwarderPort)
	}

	loggers := make([]*vr.JsonLogger, NumInstances)
	messengers := make([]*vr.JsonMessenger, NumInstances)
	replications := make([]*vr.VR, NumInstances)
	stores := make([]*kv.KVStore, NumInstances)
	servers := make([]*server.Server, NumInstances)
	for i := 0; i < NumInstances; i++ {
		filename := "logv" + strconv.Itoa(i)
		loggers[i] = vr.NewJsonLogger(&filename, false)
		messengers[i] = vr.NewJsonMessenger(":"+strconv.Itoa(peerPorts[i]), proxyPeerUris)
		replications[i] = vr.NewVR(i == 0, int64(i), messengers[i], loggers[i], ids)
		stores[i] = kv.NewKVStore(int64(i), replications[i])
		servers[i] = server.NewServer(strconv.Itoa(serverPorts[i]), stores[i])
	}

	go func() {
		if err := http.ListenAndServe(":"+strconv.Itoa(CentralForwarderPort), &RequestForwarder{}); err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		if err := http.ListenAndServe(":"+strconv.Itoa(CentralWebServerPort), &WebServer{}); err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		if err := http.ListenAndServe(":"+strconv.Itoa(CentralWebSocketPort), &WebSocketServer{}); err != nil {
			log.Fatal(err)
		}
	}()

	// Do not exit
	<-make(chan interface{})
}

type Path struct {
	From int64
	To   int64
}

type ForwardMessage struct {
	Method string
	Path string
	Message string
}

type RequestForwarder struct{}

func (s *RequestForwarder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body %v, err = %v", b, err)
	}

	messageChan <- &ForwardMessage{r.Method, r.URL.Path, string(b)}

	w.WriteHeader(http.StatusOK)
}

type WebServer struct{}

func (s *WebServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if strings.HasPrefix(r.URL.Path, "/static") {
		path := filepath.Clean(r.URL.Path[1:])
		if strings.HasPrefix(path, "static") {
			http.ServeFile(w, r, path)
			return
		}
	}

	var v = struct {
		Host string
		Data string
	}{
		"127.0.0.1:" + strconv.Itoa(CentralWebSocketPort),
		"",
	}
	homeTempl.Execute(w, &v)
}

type WebSocketServer struct{}

func (s *WebSocketServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("%v", err)
		return
	}
	go writer(ws)
	reader(ws)
}

func reader(ws *websocket.Conn) {
	defer ws.Close()
	ws.SetReadLimit(512)
	ws.SetReadDeadline(time.Now().Add(PongWait))
	ws.SetPongHandler(func(string) error { ws.SetReadDeadline(time.Now().Add(PongWait)); return nil })
	for {
		_, messageBytes, err := ws.ReadMessage()
		if err != nil {
			break
		}

		var message ForwardMessage
		json.Unmarshal(messageBytes, &message)
		forward(message)
	}
}

func forward(message ForwardMessage) {
	var path Path
	err := json.Unmarshal([]byte(message.Message), &path)
	if err != nil {
		log.Printf("Error parsing %v, err = %v", message.Message, err)
	}

	client := &http.Client{}
	req, err := http.NewRequest(message.Method, actualPeerUris[path.To]+message.Path, bytes.NewBuffer([]byte(message.Message)))
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
}

func writer(ws *websocket.Conn) {
	PingTicker := time.NewTicker(PingPeriod)
	defer func() {
		PingTicker.Stop()
		ws.Close()
	}()
	for {
		select {
		case msg := <-messageChan:
			ws.SetWriteDeadline(time.Now().Add(WriteWait))
			bytes, err := json.Marshal(*msg)
			if err != nil {
				log.Printf("Error encountered marshaling json message: %v", err)
				return
			}
			if err := ws.WriteMessage(websocket.TextMessage, bytes); err != nil {
				log.Printf("Error encountered writing message: %v", err)
				return
			}
		case <-PingTicker.C:
			ws.SetWriteDeadline(time.Now().Add(WriteWait))
			if err := ws.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				log.Printf("Error encountered sending ping: %v", err)
				return
			}
		}
	}
}
