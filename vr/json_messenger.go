package vr

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/ant0ine/go-json-rest/rest"
)

const PrepareEndpoint = "/prepare"
const PrepareOKEndpoint = "/prepareok"
const CommitEndpoint = "/commit"
const StartViewChangeEndpoint = "/startviewchange"
const DoViewChangeEndpoint = "/doviewchange"
const StartViewEndpoint = "/startview"

type PrepareMessage struct {
	from          int64
	to            int64
	clientID      int64
	requestID     int64
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

type StartViewChangeMessage struct {
	from    int64
	to      int64
	newView int64
}

type DoViewChangeMessage struct {
	from      int64
	to        int64
	newView   int64
	oldView   int64
	log       []string //TODO: Confirm format of log
	opNum     int64
	commitNum int64
}

type StartViewMessage struct {
	from      int64
	to        int64
	newView   int64
	log       []string //TODO: Confirm format of log
	opNum     int64
	commitNum int64
}

type JsonMessenger struct {
	prepareMessages         chan *PrepareMessage
	prepareOKMessages       chan *PrepareOKMessage
	commitMessages          chan *CommitMessage
	startViewChangeMessages chan *StartViewChangeMessage
	doViewChangeMessages    chan *DoViewChangeMessage
	startViewMessages       chan *StartViewMessage
	ReceiveHandler          func(w rest.ResponseWriter, r *rest.Request)
}

func NewJsonMessenger(port string) *JsonMessenger {
	m := &JsonMessenger{
		prepareMessages:         make(chan *PrepareMessage),
		prepareOKMessages:       make(chan *PrepareOKMessage),
		commitMessages:          make(chan *CommitMessage),
		startViewChangeMessages: make(chan *StartViewChangeMessage),
		doViewChangeMessages:    make(chan *DoViewChangeMessage),
		startViewMessages:       make(chan *StartViewMessage),
	}

	portStr := ":" + port

	handler := rest.ResourceHandler{
		EnableRelaxedContentType: true,
	}
	err := handler.SetRoutes(
		&rest.Route{"PUT", PrepareEndpoint, m.prepareHandler},
		&rest.Route{"PUT", PrepareOKEndpoint, m.prepareOKHandler},
		&rest.Route{"PUT", CommitEndpoint, m.commitHandler},
		&rest.Route{"PUT", StartViewChangeEndpoint, m.startViewChangeHandler},
		&rest.Route{"PUT", DoViewChangeEndpoint, m.doViewChangeHandler},
		&rest.Route{"PUT", StartViewEndpoint, m.startViewHandler},
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

func (s *JsonMessenger) SendPrepare(uri string, from int64, to int64, clientID int64, requestID int64,
	message string, primaryView int64, primaryOp int64, primaryCommit int64) (err error) {
	return send(uri, PrepareEndpoint, PrepareMessage{from, to, clientID, requestID, message, primaryView, primaryOp, primaryCommit})
}

func (s *JsonMessenger) SendPrepareOK(uri string, from int64, to int64, backupView int64, backupOp int64) (err error) {
	return send(uri, PrepareOKEndpoint, PrepareOKMessage{from, to, backupView, backupOp})
}

func (s *JsonMessenger) SendCommit(uri string, from int64, to int64, primaryView int64,
	primaryCommit int64) (err error) {
	return send(uri, CommitEndpoint, CommitMessage{from, to, primaryView, primaryCommit})
}

func (s *JsonMessenger) ReceivePrepare() (from int64, to int64, clientID int64, requestID int64, message string,
	primaryView int64, primaryOp int64, primaryCommit int64, err error) {
	msg, ok := <-s.prepareMessages
	if !ok {
		err = errors.New("Error: no more incoming entries")
		return
	}
	return msg.from, msg.to, msg.clientID, msg.requestID, msg.message, msg.primaryView, msg.primaryOp, msg.primaryCommit, err
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
	fmt.Printf("ReceiveCommit(): %v, OK = %v\n", msg, ok)
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
	fmt.Printf("Req: %v\n", *r)
	msg := CommitMessage{}
	err := r.DecodeJsonPayload(&msg)
	fmt.Printf("msg: %v, err: %v\n", msg, err)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.commitMessages <- &msg
	w.WriteHeader(http.StatusOK)
}

//-----------View change functions---------------_

func (s *JsonMessenger) SendStartViewChange(uri string, from int64, to int64, newView int64) (err error) {
	return send(uri, StartViewChangeEndpoint, StartViewChangeMessage{from, to, newView})
}

func (s *JsonMessenger) SendDoViewChange(uri string, from int64, to int64, newView int64, oldView int64, log []string, opNum int64,
	commitNum int64) (err error) {
	return send(uri, DoViewChangeEndpoint, DoViewChangeMessage{from, to, newView, oldView, log, opNum, commitNum})
}

func (s *JsonMessenger) SendStartView(uri string, from int64, to int64, newView int64, log []string, opNum int64,
	commitNum int64) (err error) {
	return send(uri, StartViewEndpoint, StartViewMessage{from, to, newView, log, opNum, commitNum})
}

func (s *JsonMessenger) ReceiveStartViewChange() (from int64, to int64, newView int64, err error) {
	msg, ok := <-s.startViewChangeMessages
	if !ok {
		err = errors.New("Error: no more incoming entries")
		return
	}
	return msg.from, msg.to, msg.newView, err
}

func (s *JsonMessenger) ReceiveDoViewChange() (from int64, to int64, newView int64, oldView int64, log []string, opNum int64,
	commitNum int64, err error) {
	msg, ok := <-s.doViewChangeMessages
	if !ok {
		err = errors.New("Error: no more incoming entries")
		return
	}
	return msg.from, msg.to, msg.newView, msg.oldView, msg.log, msg.opNum, msg.commitNum, err
}

func (s *JsonMessenger) ReceiveStartView() (from int64, to int64, newView int64, log []string, opNum int64,
	commitNum int64, err error) {
	msg, ok := <-s.startViewMessages
	if !ok {
		err = errors.New("Error: no more incoming entries")
		return
	}
	return msg.from, msg.to, msg.newView, msg.log, msg.opNum, msg.commitNum, err
}

func (s *JsonMessenger) startViewChangeHandler(w rest.ResponseWriter, r *rest.Request) {
	msg := StartViewChangeMessage{}
	err := r.DecodeJsonPayload(&msg)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.startViewChangeMessages <- &msg
	w.WriteHeader(http.StatusOK)
}

func (s *JsonMessenger) doViewChangeHandler(w rest.ResponseWriter, r *rest.Request) {
	msg := DoViewChangeMessage{}
	err := r.DecodeJsonPayload(&msg)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.doViewChangeMessages <- &msg
	w.WriteHeader(http.StatusOK)
}

func (s *JsonMessenger) startViewHandler(w rest.ResponseWriter, r *rest.Request) {
	msg := StartViewMessage{}
	err := r.DecodeJsonPayload(&msg)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.startViewMessages <- &msg
	w.WriteHeader(http.StatusOK)
}

//-----------End view change functions---------------

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
	fmt.Printf("%v %v\n", uri+endpoint, object)

	client := &http.Client{}
	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	fmt.Printf("%v %v\n", resp, err)
	return err
}
