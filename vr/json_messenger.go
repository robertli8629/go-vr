package vr

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/ant0ine/go-json-rest/rest"
)

const PrepareEndpoint = "/prepare"
const PrepareOKEndpoint = "/prepareok"
const CommitEndpoint = "/commit"

type PrepareMessage struct {
	from          int64
	to            int64
	message       string
	primaryView   int64
	primaryOp     int64
	primaryCommit int64
}

type PrepareOKMessage struct {
	from       int64
	to         int64
	backupView int64
	backupOp   int64
}

type CommitMessage struct {
	from          int64
	to            int64
	primaryView   int64
	primaryCommit int64
}

type JsonMessenger struct {
	prepareMessages   chan *PrepareMessage
	prepareOKMessages chan *PrepareOKMessage
	commitMessages    chan *CommitMessage
	ReceiveHandler    func(w rest.ResponseWriter, r *rest.Request)
}

func NewJsonMessenger(port string) *JsonMessenger {
	m := &JsonMessenger{
		prepareMessages:   make(chan *PrepareMessage),
		prepareOKMessages: make(chan *PrepareOKMessage),
		commitMessages:    make(chan *CommitMessage),
	}

	portStr := ":" + port

	handler := rest.ResourceHandler{
		EnableRelaxedContentType: true,
	}
	err := handler.SetRoutes(
		&rest.Route{"PUT", PrepareEndpoint, m.prepareHandler},
		&rest.Route{"PUT", PrepareOKEndpoint, m.prepareOKHandler},
		&rest.Route{"PUT", CommitEndpoint, m.commitHandler},
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

func (s *JsonMessenger) SendPrepare(uri string, from int64, to int64, message string, primaryView int64,
	primaryOp int64, primaryCommit int64) (err error) {
	return send(uri, PrepareEndpoint, PrepareMessage{from, to, message, primaryView, primaryOp, primaryCommit})
}

func (s *JsonMessenger) SendPrepareOK(uri string, from int64, to int64, backupView int64, backupOp int64) (err error) {
	return send(uri, PrepareOKEndpoint, PrepareOKMessage{from, to, backupView, backupOp})
}

func (s *JsonMessenger) SendCommit(uri string, from int64, to int64, primaryView int64,
	primaryCommit int64) (err error) {
	return send(uri, CommitEndpoint, CommitMessage{from, to, primaryView, primaryCommit})
}

func (s *JsonMessenger) ReceivePrepare() (from int64, to int64, message string, primaryView int64, primaryOp int64,
	primaryCommit int64, err error) {
	msg, ok := <-s.prepareMessages
	if !ok {
		err = errors.New("Error: no more incoming entries")
		return
	}
	return msg.from, msg.to, msg.message, msg.primaryView, msg.primaryOp, msg.primaryCommit, err
}

func (s *JsonMessenger) ReceivePrepareOK() (from int64, to int64, backupView int64, backupOp int64, err error) {
	msg, ok := <-s.prepareOKMessages
	if !ok {
		err = errors.New("Error: no more incoming entries")
		return
	}
	return msg.from, msg.to, msg.backupView, msg.backupOp, err
}

func (s *JsonMessenger) ReceiveCommit() (from int64, to int64, primaryView int64, primaryCommit int64, err error) {
	msg, ok := <-s.commitMessages
	if !ok {
		err = errors.New("Error: no more incoming entries")
		return
	}
	return msg.from, msg.to, msg.primaryView, msg.primaryCommit, err
}

func (s *JsonMessenger) prepareHandler(w rest.ResponseWriter, r *rest.Request) {
	log.Println("Peer Request: /prepare")
	msg := PrepareMessage{}
	err := r.DecodeJsonPayload(&msg)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.prepareMessages <- &msg
	w.WriteHeader(http.StatusOK)
}

func (s *JsonMessenger) prepareOKHandler(w rest.ResponseWriter, r *rest.Request) {
	log.Println("Peer Request: /prepareok")
	msg := PrepareOKMessage{}
	err := r.DecodeJsonPayload(&msg)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.prepareOKMessages <- &msg
	w.WriteHeader(http.StatusOK)
}

func (s *JsonMessenger) commitHandler(w rest.ResponseWriter, r *rest.Request) {
	log.Println("Peer Request: /commit")
	msg := CommitMessage{}
	err := r.DecodeJsonPayload(&msg)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.commitMessages <- &msg
	w.WriteHeader(http.StatusOK)
}

func send(uri string, endpoint string, object interface{}) (err error) {
	b, err := json.Marshal(object)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", uri+endpoint, bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	defer resp.Body.Close()

	return err
}
