package main

import (
	"fmt"
	"github.com/ant0ine/go-json-rest/rest"
	"github.com/robertli8629/cs244b_project/kv"
	"log"
	"net/http"
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

var store = kv.NewKVStore()

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
