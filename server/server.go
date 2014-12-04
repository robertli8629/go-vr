package server

import (
	"io/ioutil"
	"log"
	"net/http"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/robertli8629/go-vr/kv"
)

type Body struct {
	Value string
}

type Server struct {
	portStr string
	store   *kv.KVStore
}

func NewServer(port string, s *kv.KVStore) (server *Server) {
	portStr := ":" + port

	server = &Server{portStr: portStr, store: s}

	handler := rest.ResourceHandler{
		EnableRelaxedContentType: true,
		EnableResponseStackTrace: true,
		// Discard go-json-rest logs
		Logger: log.New(ioutil.Discard, "", 0),
	}
	err := handler.SetRoutes(
		&rest.Route{"PUT", "/object/*key", server.Put},
		&rest.Route{"GET", "/object/*key", server.Get},
		&rest.Route{"DELETE", "/object/*key", server.Delete},
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Listening messages from clients at", portStr)

	go func() {
		log.Fatal(http.ListenAndServe(server.portStr, &handler))
	}()

	return server
}

func (s *Server) Get(w rest.ResponseWriter, r *rest.Request) {
	key := r.PathParam("key")
	log.Println("Client Request: GET " + key)

	value := s.store.Get(key)

	if value == nil {
		rest.NotFound(w, r)
		log.Println("Response: Not Found")
		return
	}
	log.Println("Response: Success. Return " + *value)

	w.WriteJson(&Body{Value: *value})
}

func (s *Server) Put(w rest.ResponseWriter, r *rest.Request) {
	key := r.PathParam("key")
	body := Body{}
	err := r.DecodeJsonPayload(&body)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("Client Request: PUT " + key + " " + body.Value)

	err = s.store.Put(key, &body.Value)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		log.Println("Response: Error")
		return
	}
	log.Println("Response: Success")

	w.WriteHeader(http.StatusOK)
}

func (s *Server) Delete(w rest.ResponseWriter, r *rest.Request) {
	key := r.PathParam("key")
	log.Println("Client Request: DELETE " + key)
	s.store.Delete(key)
	log.Println("Response: Success")
	w.WriteHeader(http.StatusOK)
}
