package vr

import (
	"errors"
	"log"
	"sync"
	"time"
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
}

type ClientTableEntry struct {
	RequestID  int64
	Processing bool
	Response   error
}

type VR struct {
	GroupIDs     []int64
	GroupUris    map[int64]string
	Index        int64
	ViewNumber   int64
	Status       Status
	OpNumber     int64
	CommitNumber int64
	ClientTable  map[int64]*ClientTableEntry
	OperationTable map[int64]map[int64]bool
	Log []*string

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
	if backupOp > s.CommitNumber + 1 {
		return
	}
	quorumSize := len(s.GroupIDs) / 2 + 1
	for s.CommitNumber < s.OpNumber {
		ballot := s.OperationTable[s.CommitNumber + 1]
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
		_, _, _, primaryCommit, err := s.Messenger.ReceiveCommit()
		if err != nil {
			continue
		}

		s.commitUpTo(primaryCommit)
	}
}

func (s *VR) commitUpTo(primaryCommit int64) {
	s.lock.Lock()
	for s.CommitNumber < primaryCommit {
		s.CommitNumber++
		s.Upcall(*s.Log[s.CommitNumber])
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
					go s.Messenger.SendCommit(uri, s.Index, int64(i), s.ViewNumber, s.CommitNumber)
				}
			}
		}
		s.lock.RUnlock()
		log.Println("Finished sending Commit messages")
	}
}
