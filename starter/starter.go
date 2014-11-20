package starter

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/robertli8629/cs244b_project/kv"
	"github.com/robertli8629/cs244b_project/server"
	"github.com/robertli8629/cs244b_project/logging"
	"github.com/robertli8629/cs244b_project/vr"
)

// return a list of port numbers in the config file
func read_config() (servers []string, peers []string) {
	file, err := os.Open("config.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			servers = append(servers, fields[0])
			peers = append(peers, fields[1])
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return servers, peers
}


func Start() {

	servers, peers := read_config()
	argsWithoutProg := os.Args[1:]
	argSize := len(argsWithoutProg)

	id := 0
	if argSize == 0 {
		panic("Server index is needed as an argument.")
	}
	idStr := argsWithoutProg[0]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		panic(err)
	}
	log.Println("Id:", id)

	// sample to call read and write to log, filename convention "logsX"
	l := logging.Log{"2","3","23-key-v"}
	filename := "logs" + idStr
	logging.Write_to_log(l, filename)
	ls ,v, o := logging.Read_from_log(filename)
	log.Println(ls)
	log.Println(v)
	log.Println(o)
	

	serverPort, peerPort := servers[id], peers[id]

	isMaster := id == 0
	if isMaster {
		log.Println("This is MASTER")
	} else {
		log.Println("This is REPLICA")
	}
	
	messenger := vr.NewJsonMessenger(peerPort)

	uri := []string{}
	for i := 0; i < len(peers); i++ {
		if i != id {
			uri = append(uri, "http://127.0.0.1:" + peers[i])
		}
	}

	logger := vr.VR{IsMaster: isMaster, Messenger: messenger, PeerUris: uri}
	store := kv.NewKVStore(&logger)
	server.NewServer(serverPort, store)

	<-make(chan interface{})
}
