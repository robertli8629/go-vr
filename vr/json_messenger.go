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
const StartViewChangeEndpoint = "/startviewchange"
const DoViewChangeEndpoint = "/doviewchange"
const StartViewEndpoint = "/startview"

type PrepareMessage struct {
	From          int64
	To            int64
	ClientID      int64
	RequestID     int64
	Message       string
	PrimaryView   int64
	PrimaryOp     int64
	PrimaryCommit int64
}

type PrepareOKMessage struct {
	From       int64
	To         int64
	BackupView int64
	BackupOp   int64
}

type CommitMessage struct {
	From          int64
	To            int64
	PrimaryView   int64
	PrimaryCommit int64
}

type StartViewChangeMessage struct {
	From    int64
	To      int64
	NewView int64
}

type DoViewChangeMessage struct {
	From      int64
	To        int64
	NewView   int64
	OldView   int64
	Log       []string //TODO: Confirm format of log
	OpNum     int64
	CommitNum int64
}

type StartViewMessage struct {
	From      int64
	To        int64
	NewView   int64
	Log       []string //TODO: Confirm format of log
	OpNum     int64
	CommitNum int64
}

type JsonMessenger struct {
	prepareMessages         chan *PrepareMessage
	prepareOKMessages       chan *PrepareOKMessage
	commitMessages          chan *CommitMessage
	startViewChangeMessages chan *StartViewChangeMessage
	doViewChangeMessages    chan *DoViewChangeMessage
	startViewMessages       chan *StartViewMessage
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
	return msg.From, msg.To, msg.ClientID, msg.RequestID, msg.Message, msg.PrimaryView, msg.PrimaryOp, msg.PrimaryCommit, err
}

func (s *JsonMessenger) ReceivePrepareOK() (from int64, to int64, backupView int64, backupOp int64, err error) {
	msg, ok := <-s.prepareOKMessages
	if !ok {
		err = errors.New("Error: no more incoming entries")
		return
	}
	return msg.From, msg.To, msg.BackupView, msg.BackupOp, err
}

func (s *JsonMessenger) ReceiveCommit() (from int64, to int64, primaryView int64, primaryCommit int64, err error) {
	msg, ok := <-s.commitMessages
	if !ok {
		err = errors.New("Error: no more incoming entries")
		return
	}
	return msg.From, msg.To, msg.PrimaryView, msg.PrimaryCommit, err
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
	return msg.From, msg.To, msg.NewView, err
}

func (s *JsonMessenger) ReceiveDoViewChange() (from int64, to int64, newView int64, oldView int64, log []string, opNum int64,
	commitNum int64, err error) {
	msg, ok := <-s.doViewChangeMessages
	if !ok {
		err = errors.New("Error: no more incoming entries")
		return
	}
	return msg.From, msg.To, msg.NewView, msg.OldView, msg.Log, msg.OpNum, msg.CommitNum, err
}

func (s *JsonMessenger) ReceiveStartView() (from int64, to int64, newView int64, log []string, opNum int64,
	commitNum int64, err error) {
	msg, ok := <-s.startViewMessages
	if !ok {
		err = errors.New("Error: no more incoming entries")
		return
	}
	return msg.From, msg.To, msg.NewView, msg.Log, msg.OpNum, msg.CommitNum, err
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
	log.Printf("Message sent: PUT %v %v\n", uri+endpoint, string(b))

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}

	return err
}
