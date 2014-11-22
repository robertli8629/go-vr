package synchronous

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/ant0ine/go-json-rest/rest"
)

const endpoint = "/log"

type JsonMessenger struct {
	entries        chan *Entry
	ReceiveHandler func(w rest.ResponseWriter, r *rest.Request)
}

func NewJsonMessenger(port string) *JsonMessenger {
	m := &JsonMessenger{entries: make(chan *Entry)}

	m.ReceiveHandler = func(w rest.ResponseWriter, r *rest.Request) {
		entry := Entry{}
		err := r.DecodeJsonPayload(&entry)
		if err != nil {
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		m.entries <- &entry
		w.WriteHeader(http.StatusOK)
	}

	portStr := ":" + port

	handler := rest.ResourceHandler{
		EnableRelaxedContentType: true,
	}
	err := handler.SetRoutes(
		&rest.Route{"PUT", endpoint, m.ReceiveHandler},
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Listening messenges from peers at", portStr)

	go func() {
		log.Fatal(http.ListenAndServe(portStr, &handler))
	}()

	return m
}

func (s *JsonMessenger) SendLogEntry(uri string, entry *Entry) error {
	b, err := json.Marshal(*entry)
	if err != nil {
		panic(err)
	}

	req, err := http.NewRequest("PUT", uri+endpoint, bytes.NewBuffer(b))
	if err != nil {
		panic(err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	defer resp.Body.Close()

	return err
}

func (s *JsonMessenger) ReceiveLogEntry() (entry *Entry, err error) {
	entry, ok := <-s.entries
	if !ok {
		err = errors.New("Error: no more incoming entries")
	}
	return
}
