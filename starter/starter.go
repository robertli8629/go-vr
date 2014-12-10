package starter

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/robertli8629/go-vr/kv"
	"github.com/robertli8629/go-vr/server"
	"github.com/robertli8629/go-vr/vr"
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
	serverPorts, peerPorts := read_config()
	argsWithoutProg := os.Args[1:]
	argSize := len(argsWithoutProg)

	if argSize == 0 {
		panic("Server index is needed as an argument.")
	}
	idStr := argsWithoutProg[0]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		panic(err)
	}
	log.Println("Id:", id)

	isPrimary := id == 0
	if isPrimary {
		log.Println("This is MASTER")
	} else {
		log.Println("This is REPLICA")
	}

	ids := []int64{}
	peerUris := map[int64]string{}
	for i := int64(0); i < int64(len(peerPorts)); i++ {
		ids = append(ids, i)
		peerUris[i] = "http://127.0.0.1:" + peerPorts[i]
	}

	messenger := vr.NewJsonMessenger(":"+peerPorts[int64(id)], peerUris)

	appendOnly := true
	if argSize >= 2 && argsWithoutProg[1] == "coldstart" {
		appendOnly = false
	}

	filename := "logs" + idStr
	logger := vr.NewJsonLogger(&filename, appendOnly)

	// Using same id for VR and KV for convenience only
	replication := vr.NewVR(isPrimary, int64(id), messenger, logger, ids)
	store := kv.NewKVStore(int64(id), replication)
	server.NewServer(serverPorts[id], store)

	// test log replay
	//ls, _, _ := logging.Read_from_log(replication.Log_struct.Filename)
	//ls, _, _ := logging.Read_from_log("logs" + idStr)
	//store.ReplayLogs(ls)

	// Do not exit
	<-make(chan interface{})
}
