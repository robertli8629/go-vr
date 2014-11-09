package main

import (
	"fmt"
	"github.com/ant0ine/go-json-rest/rest"
	"log"
	"net/http"
	"strings"
	"sync"
	"os"
)

type Body struct {
	Value string
}

func main() {

	portStr := ":8080"
	argsWithoutProg := os.Args[1:]
	argSize := len(argsWithoutProg)
	fmt.Println(argSize)
	if argSize >= 1 {
		port := argsWithoutProg[0]
		portStr = ":" + string(port)
		fmt.Println(portStr)
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

var store = map[string]*string{}

var lock = sync.RWMutex{}

func Get(w rest.ResponseWriter, r *rest.Request) {
	key := r.PathParam("key")

	lock.RLock()
	var value *string
	if store[key] != nil {
		value = store[key]
	}
	lock.RUnlock()

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
	lock.Lock()
	store[key] = &body.Value
	lock.Unlock()
	w.WriteJson(&key)
}

func Delete(w rest.ResponseWriter, r *rest.Request) {
	key := r.PathParam("key")
	lock.Lock()
	delete(store, key)
	lock.Unlock()
	w.WriteHeader(http.StatusOK)
}

func List(w rest.ResponseWriter, r *rest.Request) {
	prefix := r.PathParam("prefix")
	list := []string{}
	lock.Lock()
	for key, _ := range store {
		fmt.Printf("%s\n", key)
		fmt.Printf("%s\n", prefix)
		if strings.HasPrefix(key, prefix) {
			list = append(list, key)
		}
	}
	lock.Unlock()
	w.WriteJson(list)
}

