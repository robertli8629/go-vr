package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/robertli8629/cs244b_project/kv"
)

type Body struct {
	Value string
}

// return a list of port numbers in the config file
func read_config() (list []string) {
	file, err := os.Open("config.txt")
	ret := []string{}

	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		ret = append(ret, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return ret

}

func main() {

	portStr := ":8080"
	port := 8080
	argsWithoutProg := os.Args[1:]
	argSize := len(argsWithoutProg)

	if argSize >= 1 {
		strport := argsWithoutProg[0]
		tempPort, err := strconv.Atoi(strport)
		if err == nil {
			port = tempPort
			portStr = ":" + strconv.Itoa(port)
		}
	}

	config := read_config()

	fmt.Println(config)
	fmt.Println(port)

	//For now, port 8080 is master
	if portStr == ":8080" {
		isMaster = true
		fmt.Println("This is MASTER")
	}

	handler := rest.ResourceHandler{
		EnableRelaxedContentType: true,
	}
	err := handler.SetRoutes(
		&rest.Route{"PUT", "/object/*key", Put},
		&rest.Route{"GET", "/object/*key", Get},
		&rest.Route{"DELETE", "/object/*key", Delete},
		&rest.Route{"GET", "/list/*prefix", List},
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(http.ListenAndServe(portStr, &handler))
}

var replicaList = ReadConfig()
var isMaster = false
var store = kv.NewKVStore(replicaList)

func ReadConfig() []string {

	//Replace with config file read function in future
	//Now hard coded with 2 additional replica URL
	urlList := []string{}

	for i := 0; i < 2; i++ {
		portnum := 9000 + i*100
		url := "http://127.0.0.1:" + strconv.Itoa(portnum) + "/object/"
		fmt.Println(url)
		urlList = append(urlList, url)
	}

	return urlList
}

func Get(w rest.ResponseWriter, r *rest.Request) {
	key := r.PathParam("key")

	value := store.Get(key)

	if value == nil {
		rest.NotFound(w, r)
		return
	}
	w.WriteJson(value)
}

func Put(w rest.ResponseWriter, r *rest.Request) {
	key := r.PathParam("key")
	body := Body{}
	err := r.DecodeJsonPayload(&body)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	store.Put(key, &body.Value)
	w.WriteJson(&key)

	if isMaster == true {
		//Send to replicas
		for _, element := range replicaList {
			url := element + key
			fmt.Println(url)
			var json = []byte(`{"value":"value1"}`)
			req, err := http.NewRequest("PUT", url, bytes.NewBuffer(json))
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}
			defer resp.Body.Close()
		}
	}
}

func Delete(w rest.ResponseWriter, r *rest.Request) {
	key := r.PathParam("key")
	store.Delete(key)
	w.WriteHeader(http.StatusOK)
}

func List(w rest.ResponseWriter, r *rest.Request) {
	prefix := r.PathParam("prefix")
	list := store.List(prefix)
	w.WriteJson(list)
}
