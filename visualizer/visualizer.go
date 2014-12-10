package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"log"
	"strconv"
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
var messageChan chan string

var homeTempl = template.Must(template.New("").Parse(homeHTML))
var upgrader  = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	messageChan = make(chan string, 1000)

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
		messengers[i] = vr.NewJsonMessenger(":" + strconv.Itoa(peerPorts[i]), proxyPeerUris)
		replications[i] = vr.NewVR(i == 0, int64(i), messengers[i], loggers[i], ids)
		stores[i] = kv.NewKVStore(int64(i), replications[i])
		servers[i] = server.NewServer(strconv.Itoa(serverPorts[i]), stores[i])
	}

	go func() {
		if err := http.ListenAndServe(":" + strconv.Itoa(CentralForwarderPort), &RequestForwarder{}); err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		if err := http.ListenAndServe(":" + strconv.Itoa(CentralWebServerPort), &WebServer{}); err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		if err := http.ListenAndServe(":" + strconv.Itoa(CentralWebSocketPort), &WebSocketServer{}); err != nil {
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

type RequestForwarder struct {}

func (s *RequestForwarder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body %v, err = %v", b, err)
	}
	
	messageChan <- string(b)

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

type WebServer struct {}

func (s *WebServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	var v = struct {
		Host    string
		Data    string
	}{
		"127.0.0.1:" + strconv.Itoa(CentralWebSocketPort),
		"",
	}

	homeTempl.Execute(w, &v)
}

type WebSocketServer struct {}

func (s *WebSocketServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Starting WebSocket 1")
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("%v", err)
		return
	}
	log.Printf("Starting WebSocket 2")
	go writer(ws)
	reader(ws)
}

func reader(ws *websocket.Conn) {
	defer ws.Close()
	ws.SetReadLimit(512)
	ws.SetReadDeadline(time.Now().Add(PongWait))
	ws.SetPongHandler(func(string) error { ws.SetReadDeadline(time.Now().Add(PongWait)); return nil })
	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			break
		}
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
			if err := ws.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
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

const homeHTML = `<!DOCTYPE html>
<html lang="en">
    <head>
        <title>WebSocket Example</title>
    </head>
    <body>
        <pre id="messageData">{{.Data}}</pre>
        <script type="text/javascript">
            (function() {
                var data = document.getElementById("messageData");
                var conn = new WebSocket("ws://{{.Host}}/");
                conn.onclose = function(evt) {
                    data.textContent += "Connection closed\n";
                }
                conn.onmessage = function(evt) {
                    console.log('Message received');
                    data.textContent += evt.data + "\n";
                }
            })();
        </script>
    </body>
</html>
`
