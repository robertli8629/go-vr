package vr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/robertli8629/cs244b_project/logging"
)

type Status int64

type Messenger interface {
	SendPrepare(uri string, from int64, to int64, clientID int64, requestID int64, message string,
		primaryView int64, primaryOp int64, primaryCommit int64) (err error)
	SendPrepareOK(uri string, from int64, to int64, backupView int64, backupOp int64) (err error)
	SendCommit(uri string, from int64, to int64, primaryView int64, primaryCommit int64) (err error)
	ReceivePrepare() (from int64, to int64, clientID int64, requestID int64, message string,
		primaryView int64, primaryOp int64, primaryCommit int64, err error)
	ReceivePrepareOK() (from int64, to int64, backupView int64, backupOp int64, err error)
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

type ClientTableEntry struct {
	RequestID  int64
	Processing bool
	Response   error
}

type VR struct {
	GroupIDs       []int64
	GroupUris      map[int64]string
	Index          int64
	ViewNumber     int64
	Status         Status
	OpNumber       int64
	CommitNumber   int64
	ClientTable    map[int64]*ClientTableEntry
	OperationTable map[int64]map[int64]bool
	Log            []*string

	ViewChangeViewNum        int64
	NumOfStartViewChangeRecv int64
	NumOfDoViewChangeRecv    int64
	DoViewChangeSent         bool
	Quorum                   int64 //TODO: calculate quorum
	DoViewChangeStatus       DoViewChangeStore

	IsPrimary bool
	Messenger Messenger

	Upcall func(message string) (result string)
	lock   *sync.RWMutex
}

func NewVR(isPrimary bool, index int64, messenger Messenger, ids []int64, uris map[int64]string) (s *VR) {
	s = &VR{IsPrimary: isPrimary, Index: index, OpNumber: -1, CommitNumber: -1, Messenger: messenger,
		GroupIDs: ids, GroupUris: uris}
	s.ClientTable = map[int64]*ClientTableEntry{}
	s.OperationTable = map[int64]map[int64]bool{}
	s.lock = &sync.RWMutex{}
	go s.PrepareListener()
	go s.PrepareOKListener()
	go s.CommitListener()
	go s.CommitBroadcaster()
	return s
}

func (s *VR) RegisterUpcall(callback func(message string) (result string)) {
	s.Upcall = callback
}

func (s *VR) Request(message string, clientID int64, requestID int64) (err error) {
	log.Println("Starting request to replicate: " + message)
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.IsPrimary {
		return errors.New("Error: request can only be sent to the master.")
	}

	if entry := s.ClientTable[clientID]; entry != nil {
		if requestID < entry.RequestID {
			return errors.New("Error: stale request.")
		} else if requestID == entry.RequestID {
			if entry.Processing {
				return errors.New("Error: still processing request.")
			} else {
				return entry.Response
			}
		}
	}

	s.OpNumber++
	s.ClientTable[clientID] = &ClientTableEntry{RequestID: requestID, Processing: true}
	s.OperationTable[s.OpNumber] = map[int64]bool{}
	s.Log = append(s.Log, &message)

	for i, uri := range s.GroupUris {
		if int64(i) != s.Index {
			go s.Messenger.SendPrepare(uri, clientID, requestID, s.Index, int64(i), message, s.ViewNumber,
				s.OpNumber, s.CommitNumber)
		}
	}
	log.Println("Finished sending Prepare messages")

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
	_, _, newView, err := s.Messenger.ReceiveStartViewChange()
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

func (s *VR) PrepareListener() {
	for {
		from, to, clientID, requestID, message, _, primaryOp,
			primaryCommit, err := s.Messenger.ReceivePrepare()
		// Ignore parsing/transmission error, or out-of-date op
		if err != nil || primaryOp <= s.OpNumber {
			continue
		}

		if primaryOp > s.OpNumber+1 {
			// TODO: State transfer to get missing information
		}

		// Out-of-order message
		if primaryOp != s.OpNumber+1 {
			continue
		}

		s.commitUpTo(primaryCommit)

		s.lock.Lock()
		s.OpNumber++
		s.ClientTable[clientID] = &ClientTableEntry{RequestID: requestID, Processing: true}
		s.Log = append(s.Log, &message)
		go s.Messenger.SendPrepareOK(s.GroupUris[from], to, from, s.ViewNumber, s.OpNumber)
		s.lock.Unlock()
	}
}

func (s *VR) PrepareOKListener() {
	for {
		from, _, _, backupOp, err := s.Messenger.ReceivePrepareOK()
		if err != nil {
			continue
		}

		s.updateReceivedPrepareOK(from, backupOp)
	}
}

func (s *VR) updateReceivedPrepareOK(from int64, backupOp int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Ignore commited operations
	if backupOp <= s.CommitNumber {
		return
	}

	// Update table for PrepareOK messages
	ballot, found := s.OperationTable[backupOp]
	if !found {
		return
	}
	_, found = ballot[from]
	if !found {
		ballot[from] = true
		return
	}

	// Commit operations agreed by quorum
	if backupOp > s.CommitNumber+1 {
		return
	}
	quorumSize := len(s.GroupIDs)/2 + 1
	for s.CommitNumber < s.OpNumber {
		ballot := s.OperationTable[s.CommitNumber+1]
		if len(ballot) >= quorumSize {
			s.CommitNumber++
			delete(s.OperationTable, backupOp)
		} else {
			break
		}
	}

	return
}

func (s *VR) CommitListener() {
	for {
		from, to, primaryView, primaryCommit, err := s.Messenger.ReceiveCommit()
		fmt.Printf("%v %v %v %v %v\n", from, to, primaryView, primaryCommit, err)

		if err != nil {
			continue
		}

		s.commitUpTo(primaryCommit)
	}
}

func (s *VR) commitUpTo(primaryCommit int64) {
	s.lock.Lock()
	fmt.Printf("%v %v %v\n", s.CommitNumber, primaryCommit, s.Log)
	for s.CommitNumber < primaryCommit {
		s.CommitNumber++
		if s.Upcall != nil {
			s.Upcall(*s.Log[s.CommitNumber])
		}
	}
	s.lock.Unlock()
}

func (s *VR) CommitBroadcaster() {
	ticker := time.NewTicker(time.Millisecond * 500)
	for {
		<-ticker.C
		s.lock.RLock()
		if s.IsPrimary {
			for i, uri := range s.GroupUris {
				if int64(i) != s.Index {
					fmt.Printf("%v %v %v %v %v\n", uri, s.Index, int64(i), s.ViewNumber, s.CommitNumber)
					go s.Messenger.SendCommit(uri, s.Index, int64(i), s.ViewNumber, s.CommitNumber)
				}
			}
			log.Println("Finished sending Commit")
		}
		s.lock.RUnlock()
	}
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
	from, _, recvNewView, recvOldView, recvLog, recvOpNum, recvCommitNum, err := s.Messenger.ReceiveDoViewChange()
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
	from, _, recvNewView, _, recvOpNum, _, err := s.Messenger.ReceiveStartView()
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
