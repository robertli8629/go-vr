package vr

import (
	"errors"
	"log"

	"github.com/robertli8629/cs244b_project/logging"
)

type Status int64

type Messenger interface {
	SendPrepare(uri string, from int64, to int64, message string, primaryView int64, primaryOp int64,
		primaryCommit int64) (err error)
	SendPrepareOK(uri string, from int64, to int64, backupView int64, backupOp int64, backupReplica int64) (err error)
	SendCommit(uri string, from int64, to int64, primaryView int64, primaryCommit int64) (err error)
	ReceivePrepare() (from int64, to int64, message string, primaryView int64, primaryOp int64,
		primaryCommit int64, err error)
	ReceivePrepareOK() (from int64, to int64, backupView int64, backupOp int64, backupReplica int64, err error)
	ReceiveCommit() (from int64, to int64, primaryView int64, primaryCommit int64, err error)
	//TODO: Confirm format of log
	SendStartViewChange(uri string, from int64, to int64, newView int64) (err error)
	SendDoViewChange(uri string, from int64, to int64, newView int64, oldView int64, log []string, opNum int64,
		commitNum int64) (err error)
	SendStartView(uri string, from int64, to int64, newView int64, log []string, opNum int64, commitNum int64) (err error)
	ReceiveStartViewChange() (from int64, to int64, newView int64, err error)
	ReceiveDoViewChange() (from int64, to int64, newView int64, oldView int64, log []string, opNum int64, commitNum int64, err error)
	ReceiveStartView() (from int64, to int64, newView int64, log []string, opNum int64, commitNum int64, err error)
}

type DoViewChangeStore struct {
	LargestCommitNum int64
	BestLogHeard     []string
	BestLogOpNum     int64
	BestLogViewNum   int64
}

type VR struct {
	GroupUris  []string
	Index      int64
	ViewNumber int64
	Status     Status
	OpNumber   int64
	//Log                []*Entry
	CommitNumber             int64
	ClientRequestTable       map[int64]int64
	ClientResultTable        map[int64]error
	ViewChangeViewNum        int64
	NumOfStartViewChangeRecv int64
	NumOfDoViewChangeRecv    int64
	DoViewChangeSent         bool
	Quorum                   int64 //TODO: calculate quorum
	DoViewChangeStatus       DoViewChangeStore

	IsPrimary bool
	Messenger Messenger
}

func (s *VR) Request(op string, clientId int64, requestId int64) (err error) {
	if !s.IsPrimary {
		return errors.New("Error: request can only be sent to the master.")
	}

	for i, uri := range s.GroupUris {
		if int64(i) != s.Index {
			go s.Messenger.SendPrepare(uri, s.Index, int64(i), op, s.ViewNumber, s.OpNumber, s.CommitNumber)
		}
	}

	for i, uri := range s.GroupUris {
		if int64(i) != s.Index {
			err := s.Messenger.SendCommit(uri, s.Index, int64(i), s.ViewNumber, s.CommitNumber)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *VR) StartViewChange(newView int64) (err error) {
	//Initiate view change
	//TODO: Change status
	if s.Status != 002 && newView > s.ViewNumber {
		s.Status = 002 // TODO: Change status
		log.Println("Start view change for new view number", newView)
		for i, uri := range s.GroupUris {
			if int64(i) != s.Index {
				s.ViewChangeViewNum = newView
				err := s.Messenger.SendStartViewChange(uri, s.Index, int64(i), s.ViewChangeViewNum)
				if err != nil {
					return err
				}
			}
		}
		s.NumOfStartViewChangeRecv++ //Increment for the one sent to ownself
		s.CheckStartViewChangeQuorum()
	}
	return nil
}

func (s *VR) ReceiveStartViewChange() (err error) {
	//TODO: Loop through all start view change messages
	from, to, newView, err := s.Messenger.ReceiveStartViewChange()
	if err != nil {
		return err
	}

	//TODO: Check status
	if s.Status != 002 && newView > s.ViewNumber {
		log.Println("Hear startviewchange msg for first time")
		s.NumOfStartViewChangeRecv++
		s.StartViewChange(newView)
		//TODO: Terminate timer?
	} else if s.Status == 002 && (newView == s.ViewChangeViewNum) {
		log.Println("Hear startviewchange msg")
		s.NumOfStartViewChangeRecv++
	}
	//Optional: Else if status = viewchange && new view number > viewchange viewnum, this is an even later view change. Update view, call s.Messenger.SendStartViewChange & reset count.

	err = s.CheckStartViewChangeQuorum()

	return err
}

func (s *VR) CheckStartViewChangeQuorum() (err error) {
	//If  NumOfStartViewChangeRecv > quorum, call SendDoViewChange()
	if s.NumOfStartViewChangeRecv >= s.Quorum && !(s.DoViewChangeSent) {
		ownLog, _, _ := logging.Read_from_log("") //TODO: get logs
		i := 2                                    //TODO!
		uri := "127.0.0.1:8100"                   //TODO: Get the index & IP of the new leader from config

		err := s.Messenger.SendDoViewChange(uri, s.Index, int64(i), s.ViewChangeViewNum, s.ViewNumber, ownLog, s.OpNumber, s.CommitNumber)
		if err != nil {
			return err
		}
		log.Println("Got quorum for startviewchange msg. Sent doviewchange to new primary")
		s.DoViewChangeSent = true
	}
	return nil
}

func (s *VR) CheckDoViewChangeQuorum() (err error) {
	if s.NumOfDoViewChangeRecv >= s.Quorum {
		err := s.StartView()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *VR) ReceiveDoViewChange() (err error) {
	//TODO: Loop through all do view change messages
	from, to, recvNewView, recvOldView, recvLog, recvOpNum, recvCommitNum, err := s.Messenger.ReceiveDoViewChange()
	if err != nil {
		return err
	}

	log.Println("Hear doviewchange msg")

	//Initiate view change - TODO: Change status to viewchange, call startviewchange}
	if s.Status != 002 && recvNewView > s.ViewNumber {
		err = s.StartViewChange(recvNewView)
		if err != nil {
			return err
		}
	}

	//Increment number of DoViewChange msg recvd
	//TODO: change status
	if s.Status == 002 && s.ViewChangeViewNum == recvNewView {
		s.NumOfDoViewChangeRecv++
		//Store logs if it is more updated than any logs previously heard
		//Check log view number and op number
		if (recvOldView > s.DoViewChangeStatus.BestLogViewNum) || (recvOldView == s.DoViewChangeStatus.BestLogViewNum && recvOpNum > s.DoViewChangeStatus.BestLogOpNum) {
			s.DoViewChangeStatus.BestLogOpNum = recvOpNum
			s.DoViewChangeStatus.BestLogViewNum = recvOldView
			s.DoViewChangeStatus.BestLogHeard = recvLog
			log.Println("Hear better log from node ", from)
		}

		if recvCommitNum > s.DoViewChangeStatus.LargestCommitNum {
			s.DoViewChangeStatus.LargestCommitNum = recvCommitNum
		}

		s.CheckDoViewChangeQuorum()
	}
	return nil
}

func (s *VR) StartView() (err error) {
	//New primary sets own states for new view
	s.ViewNumber = s.ViewChangeViewNum
	s.OpNumber = s.DoViewChangeStatus.BestLogOpNum
	s.CommitNumber = s.DoViewChangeStatus.LargestCommitNum
	//s.Log = s.DoViewChangeStatus.BestLogHeard //TODO: Replace own log with the best heard
	s.Status = 001 //TODO: Set to normal status

	//Reset all viewchange states
	s.ViewChangeViewNum = 0
	s.NumOfStartViewChangeRecv = 0
	s.NumOfDoViewChangeRecv = 0
	s.DoViewChangeSent = false
	s.DoViewChangeStatus.BestLogOpNum = 0
	s.DoViewChangeStatus.BestLogViewNum = 0
	s.DoViewChangeStatus.LargestCommitNum = 0
	s.DoViewChangeStatus.BestLogHeard = nil
	//TODO: Restart timer for view change

	ownLog, _, _ := logging.Read_from_log("") //TODO: get logs
	for i, uri := range s.GroupUris {
		if int64(i) != s.Index {
			err := s.Messenger.SendStartView(uri, s.Index, int64(i), s.ViewNumber, ownLog, s.OpNumber, s.CommitNumber)
			if err != nil {
				return err
			}
		}
	}
	log.Println("Got quorum for doviewchange msg. Sent startview")
	log.Println("New view number: ", s.ViewNumber)

	return nil
}

func (s *VR) ReceiveStartView() (err error) {
	//TODO: Loop through all start view messages
	from, to, recvNewView, recvLog, recvOpNum, recvCommitNum, err := s.Messenger.ReceiveStartView()
	if err != nil {
		return err
	}

	s.OpNumber = recvOpNum
	s.ViewNumber = recvNewView
	//s.log = recvLog //TODO:Replace own log with new primary log
	s.Status = 001 //TODO: Set status to normal

	//TODO: Send prepareok for all non-committed operations
	//TODO: Execute committed operations that have not previously been commited at this node i.e. commit up till recvCommitNum
	//TODO: Update client table if needed

	//Reset all viewchange states
	s.ViewChangeViewNum = 0
	s.NumOfStartViewChangeRecv = 0
	s.NumOfDoViewChangeRecv = 0
	s.DoViewChangeSent = false
	s.DoViewChangeStatus.BestLogOpNum = 0
	s.DoViewChangeStatus.BestLogViewNum = 0
	s.DoViewChangeStatus.LargestCommitNum = 0
	s.DoViewChangeStatus.BestLogHeard = nil
	//TODO: Restart timer for view change

	log.Println("Received start view from new primary - node ", from)
	log.Println("New view number: ", s.ViewNumber)

	return nil
}
