package vr

import (
	"errors"
	"log"
)

type Status int64

type Messenger interface {
	SendPrepare(uri string, from int64, to int64, message string, primaryView int64, primaryOp int64,
		primaryCommit int64) (err error)
	SendPrepareOK(uri string, from int64, to int64, backupView int64, backupOp int64) (err error)
	SendCommit(uri string, from int64, to int64, primaryView int64, primaryCommit int64) (err error)
	ReceivePrepare() (from int64, to int64, message string, primaryView int64, primaryOp int64,
		primaryCommit int64, err error)
	ReceivePrepareOK() (from int64, to int64, backupView int64, backupOp int64, err error)
	ReceiveCommit() (from int64, to int64, primaryView int64, primaryCommit int64, err error)
}

type VR struct {
	GroupUris  []string
	Index      int64
	ViewNumber int64
	Status     Status
	OpNumber   int64
	//Log                []*Entry
	CommitNumber       int64
	ClientRequestTable map[int64]int64
	ClientResultTable  map[int64]error

	IsPrimary bool
	Messenger Messenger

	Upcall func(message string) (result string)
}

func (s *VR) RegisterUpcall(callback func(message string) (result string)) {
	s.Upcall = callback
}

func (s *VR) Request(op string, clientId int64, requestId int64) (err error) {
	log.Println("Starting request to replicate: " + op)
	if !s.IsPrimary {
		return errors.New("Error: request can only be sent to the master.")
	}

	for i, uri := range s.GroupUris {
		if int64(i) != s.Index {
			go s.Messenger.SendPrepare(uri, s.Index, int64(i), op, s.ViewNumber, s.OpNumber, s.CommitNumber)
		}
	}
	log.Println("Finished sending Prepare messages")

	for i, uri := range s.GroupUris {
		if int64(i) != s.Index {
			go s.Messenger.SendCommit(uri, s.Index, int64(i), s.ViewNumber, s.CommitNumber)
		}
	}
	log.Println("Finished sending Commit messages")

	return nil
}

func (s *VR) PrepareListener() {
	for {
		from, to, _, _, _, _, err := s.Messenger.ReceivePrepare()
		if err != nil {
			panic(err)
		}

		// log message
		go s.Messenger.SendPrepareOK(s.GroupUris[from], to, from, s.ViewNumber, s.OpNumber)
	}
}

func (s *VR) CommitListener() {
	for {
		_, _, _, _, err := s.Messenger.ReceiveCommit()
		if err != nil {
			panic(err)
		}

		// log message
		//result := s.Upcall(message)
	}
}
