package vr

import (
	"bytes"
	"encoding/json"
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
const RecoveryEndpoint = "/recovery"
const RecoveryResponseEndpoint = "/recoveryresponse"
const StartRecoveryEndpoint = "/startrecovery"

type PrepareMessage struct {
	From          int64
	To            int64
	Op            *Operation
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
	Log       []*LogEntry
	OpNum     int64
	CommitNum int64
}

type StartViewMessage struct {
	From      int64
	To        int64
	NewView   int64
	Log       []*LogEntry
	OpNum     int64
	CommitNum int64
}

type RecoveryMessage struct {
	From          int64
	To            int64
	Nonce         int64
	LastCommitNum int64
}

type RecoveryResponseMessage struct {
	From      int64
	To        int64
	ViewNum   int64
	Nonce     int64
	Log       []*LogEntry
	OpNum     int64
	CommitNum int64
	IsPrimary bool
}

type JsonMessenger struct {
	ID                      int64
	Uris                    map[int64]string
	ReceivePrepare          func(from int64, to int64, op *Operation, primaryView int64, primaryOp int64, primaryCommit int64)
	ReceivePrepareOK        func(from int64, to int64, backupView int64, backupOp int64)
	ReceiveCommit           func(from int64, to int64, primaryView int64, primaryCommit int64)
	ReceiveStartViewChange  func(from int64, to int64, newView int64)
	ReceiveDoViewChange     func(from int64, to int64, newView int64, oldView int64, log []*LogEntry, opNum int64, commitNum int64)
	ReceiveStartView        func(from int64, to int64, newView int64, log []*LogEntry, opNum int64, commitNum int64)
	ReceiveRecovery         func(from int64, to int64, nonce int64, lastCommitNum int64)
	ReceiveRecoveryResponse func(from int64, to int64, viewNum int64, nonce int64, log []*LogEntry, opNum int64, commitNum int64, isPrimary bool)
	ReceiveStartRecovery    func()
}

func NewJsonMessenger(addr string, uris map[int64]string) *JsonMessenger {
	m := &JsonMessenger{
		Uris: uris,
	}

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
		&rest.Route{"PUT", RecoveryEndpoint, m.recoveryHandler},
		&rest.Route{"PUT", RecoveryResponseEndpoint, m.recoveryResponseHandler},
		&rest.Route{"PUT", StartRecoveryEndpoint, m.startRecoveryHandler},
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Listening messenges from peers at", addr)

	go func() {
		log.Fatal(http.ListenAndServe(addr, &handler))
	}()

	return m
}

func (s *JsonMessenger) RegisterReceiveCallback(
	receivePrepare func(from int64, to int64, op *Operation, primaryView int64, primaryOp int64, primaryCommit int64),
	receivePrepareOK func(from int64, to int64, backupView int64, backupOp int64),
	receiveCommit func(from int64, to int64, primaryView int64, primaryCommit int64),
	receiveStartViewChange func(from int64, to int64, newView int64),
	receiveDoViewChange func(from int64, to int64, newView int64, oldView int64, log []*LogEntry, opNum int64, commitNum int64),
	receiveStartView func(from int64, to int64, newView int64, log []*LogEntry, opNum int64, commitNum int64),
	receiveRecovery func(from int64, to int64, nonce int64, lastCommitNum int64),
	receiveRecoveryResponse func(from int64, to int64, viewNum int64, nonce int64, log []*LogEntry, opNum int64, commitNum int64, isPrimary bool),
	receiveStartRecovery func()) {
	s.ReceivePrepare = receivePrepare
	s.ReceivePrepareOK = receivePrepareOK
	s.ReceiveCommit = receiveCommit
	s.ReceiveStartViewChange = receiveStartViewChange
	s.ReceiveDoViewChange = receiveDoViewChange
	s.ReceiveStartView = receiveStartView
	s.ReceiveRecovery = receiveRecovery
	s.ReceiveRecoveryResponse = receiveRecoveryResponse
	s.ReceiveStartRecovery = receiveStartRecovery
}

func (s *JsonMessenger) SendPrepare(from int64, to int64, op *Operation, primaryView int64,
	primaryOp int64, primaryCommit int64) (err error) {
	return send(s.Uris[to], PrepareEndpoint, PrepareMessage{from, to, op, primaryView, primaryOp, primaryCommit})
}

func (s *JsonMessenger) SendPrepareOK(from int64, to int64, backupView int64, backupOp int64) (err error) {
	return send(s.Uris[to], PrepareOKEndpoint, PrepareOKMessage{from, to, backupView, backupOp})
}

func (s *JsonMessenger) SendCommit(from int64, to int64, primaryView int64,
	primaryCommit int64) (err error) {
	return send(s.Uris[to], CommitEndpoint, CommitMessage{from, to, primaryView, primaryCommit})
}

func (s *JsonMessenger) prepareHandler(w rest.ResponseWriter, r *rest.Request) {
	log.Println("Peer Request: /prepare")
	msg := PrepareMessage{}
	err := r.DecodeJsonPayload(&msg)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	go s.ReceivePrepare(msg.From, msg.To, msg.Op, msg.PrimaryView, msg.PrimaryOp, msg.PrimaryCommit)
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
	go s.ReceivePrepareOK(msg.From, msg.To, msg.BackupView, msg.BackupOp)
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
	go s.ReceiveCommit(msg.From, msg.To, msg.PrimaryView, msg.PrimaryCommit)
	w.WriteHeader(http.StatusOK)
}

//-----------View change functions---------------

func (s *JsonMessenger) SendStartViewChange(from int64, to int64, newView int64) (err error) {
	return send(s.Uris[to], StartViewChangeEndpoint, StartViewChangeMessage{from, to, newView})
}

func (s *JsonMessenger) SendDoViewChange(from int64, to int64, newView int64, oldView int64, log []*LogEntry, opNum int64,
	commitNum int64) (err error) {
	return send(s.Uris[to], DoViewChangeEndpoint, DoViewChangeMessage{from, to, newView, oldView, log, opNum, commitNum})
}

func (s *JsonMessenger) SendStartView(from int64, to int64, newView int64, log []*LogEntry, opNum int64,
	commitNum int64) (err error) {
	return send(s.Uris[to], StartViewEndpoint, StartViewMessage{from, to, newView, log, opNum, commitNum})
}

func (s *JsonMessenger) startViewChangeHandler(w rest.ResponseWriter, r *rest.Request) {
	msg := StartViewChangeMessage{}
	err := r.DecodeJsonPayload(&msg)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	go s.ReceiveStartViewChange(msg.From, msg.To, msg.NewView)
	w.WriteHeader(http.StatusOK)
}

func (s *JsonMessenger) doViewChangeHandler(w rest.ResponseWriter, r *rest.Request) {
	msg := DoViewChangeMessage{}
	err := r.DecodeJsonPayload(&msg)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	go s.ReceiveDoViewChange(msg.From, msg.To, msg.NewView, msg.OldView, msg.Log, msg.OpNum, msg.CommitNum)
	w.WriteHeader(http.StatusOK)
}

func (s *JsonMessenger) startViewHandler(w rest.ResponseWriter, r *rest.Request) {
	msg := StartViewMessage{}
	err := r.DecodeJsonPayload(&msg)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	go s.ReceiveStartView(msg.From, msg.To, msg.NewView, msg.Log, msg.OpNum, msg.CommitNum)
	w.WriteHeader(http.StatusOK)
}

func (s *JsonMessenger) startRecoveryHandler(w rest.ResponseWriter, r *rest.Request) {
	go s.ReceiveStartRecovery()
	w.WriteHeader(http.StatusOK)
}

//-----------End view change functions---------------

//-----------Start recovery functions----------------

func (s *JsonMessenger) SendRecovery(from int64, to int64, nonce int64, lastCommitNum int64) (err error) {
	return send(s.Uris[to], RecoveryEndpoint, RecoveryMessage{from, to, nonce, lastCommitNum})
}

func (s *JsonMessenger) SendRecoveryResponse(from int64, to int64, viewNum int64, nonce int64, log []*LogEntry, opNum int64,
	commitNum int64, isPrimary bool) (err error) {
	return send(s.Uris[to], RecoveryResponseEndpoint, RecoveryResponseMessage{from, to, viewNum, nonce, log, opNum, commitNum, isPrimary})
}

func (s *JsonMessenger) recoveryHandler(w rest.ResponseWriter, r *rest.Request) {
	msg := RecoveryMessage{}
	err := r.DecodeJsonPayload(&msg)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	go s.ReceiveRecovery(msg.From, msg.To, msg.Nonce, msg.LastCommitNum)
	w.WriteHeader(http.StatusOK)
}

func (s *JsonMessenger) recoveryResponseHandler(w rest.ResponseWriter, r *rest.Request) {
	msg := RecoveryResponseMessage{}
	err := r.DecodeJsonPayload(&msg)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	go s.ReceiveRecoveryResponse(msg.From, msg.To, msg.ViewNum, msg.Nonce, msg.Log, msg.OpNum, msg.CommitNum, msg.IsPrimary)
	w.WriteHeader(http.StatusOK)
}

//-----------End recovery functions------------------

func send(uri string, endpoint string, object interface{}) (err error) {
	b, err := json.Marshal(object)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", uri+endpoint, bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	//log.Printf("Message sent: PUT %v %v\n", uri+endpoint, string(b))

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}

	return err
}
