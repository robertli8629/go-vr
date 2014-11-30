package starter

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
	//"fmt"

	"github.com/robertli8629/cs244b_project/kv"
	"github.com/robertli8629/cs244b_project/logging"
	"github.com/robertli8629/cs244b_project/server"
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

	var id int
	//var id64 int64
	id = 0
	if argSize == 0 {
		panic("Server index is needed as an argument.")
	}
	idStr := argsWithoutProg[0]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		panic(err)
	}
	//id64 = int64(id)
	log.Println("Id:", id)

	serverPort, peerPort := servers[id], peers[id]

	isPrimary := id == 0
	if isPrimary {
		log.Println("This is MASTER")
	} else {
		log.Println("This is REPLICA")
	}

	if argSize >= 2 && argsWithoutProg[1] == "coldstart" {
		log.Println("initializing log")
		logging.Replace_logs("logs"+idStr, make([]string, 0))
	}

	messenger := vr.NewJsonMessenger(peerPort)

	//uri := []string{}
	ids := []int64{}
	uris := map[int64]string{}
	for i := int64(0); i < int64(len(peers)); i++ {
		//uri = append(uri, "http://127.0.0.1:"+peers[i])
		ids = append(ids, i)
		uris[i] = "http://127.0.0.1:" + peers[i]
	}

	//filename := fmt.Sprintf("logs%d", id64)
	//log_struct := logging.Log_struct{Filename: filename}
	//replication := vr.VR{IsPrimary: isPrimary, Messenger: messenger, GroupUris: uri, Index: id64, Log_struct: &log_struct}
	//store := kv.NewKVStore(&replication)

	// Using same id for VR and KV for convenience only
	replication := vr.NewVR(isPrimary, int64(id), messenger, ids, uris)
	store := kv.NewKVStore(int64(id), replication)
	server.NewServer(serverPort, store)

	// test log replay
	//ls, _, _ := logging.Read_from_log(replication.Log_struct.Filename)
	//ls, _, _ := logging.Read_from_log("logs" + idStr)
	//store.ReplayLogs(ls)

	// Do not exit
	<-make(chan interface{})
}
