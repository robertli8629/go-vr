package vr

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"
)

type Status int64

const STATUS_NORMAL = 100
const STATUS_VIEWCHANGE = 200
const STATUS_RECOVERY = 300

const HEARTBEAT_INTERVAL_MS = 5000
const VIEWCHANGE_TIMEOUT_MULTIPLE = 2 //Viewchange start in multiple of heartbeat interval

type Messenger interface {
	RegisterReceiveCallback(
		receivePrepare func(from int64, to int64, op *Operation, primaryView int64, primaryOp int64, primaryCommit int64),
		receivePrepareOK func(from int64, to int64, backupView int64, backupOp int64),
		receiveCommit func(from int64, to int64, primaryView int64, primaryCommit int64),
		receiveStartViewChange func(from int64, to int64, newView int64),
		receiveDoViewChange func(from int64, to int64, newView int64, oldView int64, log []*LogEntry, opNum int64, commitNum int64),
		receiveStartView func(from int64, to int64, newView int64, log []*LogEntry, opNum int64, commitNum int64),
		receiveRecovery func(from int64, to int64, nonce int64, lastCommitNum int64),
		receiveRecoveryResponse func(from int64, to int64, viewNum int64, nonce int64, log []*LogEntry, opNum int64, commitNum int64, isPrimary bool),
		ReceiveStartRecovery func())

	SendPrepare(from int64, to int64, op *Operation, primaryView int64, primaryOp int64, primaryCommit int64) (err error)
	SendPrepareOK(from int64, to int64, backupView int64, backupOp int64) (err error)
	SendCommit(from int64, to int64, primaryView int64, primaryCommit int64) (err error)
	SendStartViewChange(from int64, to int64, newView int64) (err error)
	SendDoViewChange(from int64, to int64, newView int64, oldView int64, log []*LogEntry, opNum int64,
		commitNum int64) (err error)
	SendStartView(from int64, to int64, newView int64, log []*LogEntry, opNum int64, commitNum int64) (err error)
	SendRecovery(from int64, to int64, nonce int64, lastCommitNum int64) (err error)
	SendRecoveryResponse(from int64, to int64, viewNum int64, nonce int64, log []*LogEntry, opNum int64, commitNum int64, isPrimary bool) (err error)
}

type Logger interface {
	Append(entry *LogEntry) (err error)
	ReadAll() (log []*LogEntry)
}

type Operation struct {
	ClientID  int64
	RequestID int64
	Message   *string
}

type LogEntry struct {
	ViewNumber int64
	OpNumber   int64
	Op         *Operation
	ResultChan *chan interface{} `json:"-"`
}

type ClientTableEntry struct {
	Op         *Operation
	Processing bool
	Result     interface{}
}

type DoViewChangeStore struct {
	LargestCommitNum int64
	BestLogHeard     []*LogEntry
	BestLogOpNum     int64
	BestLogViewNum   int64
}

type RecoveryStore struct {
	RecoveryNonce        int64
	NumOfRecoveryRspRecv int64
	LargestViewSeen      int64
	PrimaryId            int64
	LogRecv              []*LogEntry
	PrimaryViewNum       int64
	PrimaryOpNum         int64
	PrimaryCommitNum     int64
	RecoveryRestartTimer *time.Timer
}

type VR struct {
	GroupIDs           []int64
	QuorumSize         int64
	Index              int64
	ViewNumber         int64
	Status             Status
	OpNumber           int64
	CommitNumber       int64
	ClientTable        map[int64]*ClientTableEntry
	PrepareBallotTable map[int64]map[int64]bool
	Log                []*LogEntry
	Logger             Logger

	ViewChangeViewNum        int64
	TriggerViewNum           int64
	NumOfStartViewChangeRecv int64
	NumOfDoViewChangeRecv    int64
	DoViewChangeSent         bool
	DoViewChangeStatus       DoViewChangeStore
	RecoveryStatus           RecoveryStore
	HeartbeatTimer           *time.Timer
	ViewChangeRestartTimer   *time.Timer

	IsPrimary bool
	Messenger Messenger

	Upcall         func(op *Operation) (result interface{})
	TransferResult func(op *Operation, result interface{})
	lock           *sync.RWMutex
}

func NewVR(isPrimary bool, index int64, messenger Messenger, logger Logger, ids []int64) (s *VR) {
	s = &VR{IsPrimary: isPrimary, Index: index, OpNumber: -1, CommitNumber: -1, Messenger: messenger, Logger: logger,
		GroupIDs: ids, DoViewChangeSent: false, ViewChangeRestartTimer: nil}
	s.ClientTable = map[int64]*ClientTableEntry{}
	s.PrepareBallotTable = map[int64]map[int64]bool{}
	s.QuorumSize = int64(len(s.GroupIDs)/2 + 1)
	s.DoViewChangeStatus = DoViewChangeStore{BestLogOpNum: -1, LargestCommitNum: -1, BestLogViewNum: -1, BestLogHeard: nil}
	s.RecoveryStatus = RecoveryStore{LargestViewSeen: -1, PrimaryId: -1, PrimaryViewNum: -2, LogRecv: nil, PrimaryOpNum: -1, PrimaryCommitNum: -1}
	rand.Seed(time.Now().UTC().UnixNano() + s.Index)
	s.HeartbeatTimer = time.NewTimer(s.getTimerInterval())
	s.lock = &sync.RWMutex{}
	s.Status = STATUS_NORMAL
	s.Messenger.RegisterReceiveCallback(s.ReceivePrepare, s.ReceivePrepareOK, s.ReceiveCommit, s.ReceiveStartViewChange,
		s.ReceiveDoViewChange, s.ReceiveStartView, s.ReceiveRecovery, s.ReceiveRecoveryResponse, s.ReceiveStartRecovery)
	go s.CommitBroadcaster()
	go s.HeartbeatTimeout()
	return s
}

func (s *VR) InitializeService(callback func(op *Operation) (result interface{})) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.Upcall = callback
	s.Log = s.Logger.ReadAll()
	s.OpNumber = int64(len(s.Log)) - 1
	s.playLogUntil(int64(len(s.Log)))
}

func (s *VR) RequestAsync(op *Operation) <-chan interface{} {
	s.lock.Lock()
	defer s.lock.Unlock()

	log.Printf("Starting to replicate requestId=%v, message=%v from clientID=%v\n", op.RequestID, *op.Message, op.ClientID)

	resultChan := make(chan interface{}, 1)

	if !s.IsPrimary {
		resultChan <- errors.New("Error: request can only be sent to the master.")
		return resultChan
	}

	if entry := s.ClientTable[op.ClientID]; entry != nil {
		log.Printf("Previous request from client: %v\n", *entry)
		if op.RequestID < entry.Op.RequestID {
			resultChan <- errors.New("Error: stale request.")
			return resultChan
		} else if op.RequestID == entry.Op.RequestID {
			if entry.Processing {
				resultChan <- errors.New("Error: still processing request.")
				return resultChan
			} else {
				resultChan <- entry.Result
				return resultChan
			}
		}
	}

	s.OpNumber++
	s.ClientTable[op.ClientID] = &ClientTableEntry{Op: op, Processing: true}
	s.PrepareBallotTable[s.OpNumber] = map[int64]bool{}
	s.Log = append(s.Log, &LogEntry{s.ViewNumber, s.OpNumber, op, &resultChan})

	for _, to := range s.GroupIDs {
		if to != s.Index {
			go s.Messenger.SendPrepare(s.Index, to, op, s.ViewNumber, s.OpNumber, s.CommitNumber)
		}
	}
	log.Println("Finished sending Prepare messages")

	return resultChan
}

func (s *VR) ReceivePrepare(from int64, to int64, op *Operation, primaryView int64, primaryOp int64, primaryCommit int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if primaryView > s.ViewNumber {
		//If this node also thinks it is a prmary, will do recovery
		go s.StartRecovery()
		return
	}

	if primaryOp > s.OpNumber+1 {
		go s.StartRecovery()
		return
	}

	// Out-of-order message
	if primaryOp != s.OpNumber+1 {
		return
	}

	//TODO: Handle msg from older views

	s.commitUpTo(primaryCommit)

	s.OpNumber++
	s.ClientTable[op.ClientID] = &ClientTableEntry{Op: op, Processing: true}
	s.Log = append(s.Log, &LogEntry{s.ViewNumber, s.OpNumber, op, nil})
	go s.Messenger.SendPrepareOK(to, from, s.ViewNumber, s.OpNumber)

	s.ResetHeartbeatTimer()
}

func (s *VR) ReceivePrepareOK(from int64, to int64, backupViewNum int64, backupOpNum int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	log.Printf("Received PrepareOK from node %v (OpNumber = %v). Master CommitNumber = %v, OpNumber = %v",
		from, backupOpNum, s.CommitNumber, s.OpNumber)
	// Ignore commited operations
	if backupOpNum <= s.CommitNumber {
		return
	}

	// Update table for PrepareOK messages
	ballot, found := s.PrepareBallotTable[backupOpNum]
	if !found {
		return
	}
	_, found = ballot[from]
	if !found {
		ballot[from] = true
	}

	// Commit operations agreed by quorum
	if backupOpNum > s.CommitNumber+1 {
		return
	}

	for s.CommitNumber < s.OpNumber {
		ballot := s.PrepareBallotTable[s.CommitNumber+1]
		if int64(len(ballot)) >= (s.QuorumSize - 1) {
			s.CommitNumber = s.executeNextOp()
			delete(s.PrepareBallotTable, s.CommitNumber)
		} else {
			break
		}
	}

	log.Printf("Processed PrepareOK")

	s.ResetHeartbeatTimer()
}

func (s *VR) ReceiveCommit(from int64, to int64, primaryView int64, primaryCommit int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if primaryView > s.ViewNumber {
		go s.StartRecovery()
		return
	} else if primaryCommit >= s.CommitNumber+2 {
		//If fall behind in the same view by more than 2 commit numbers, recover.
		go s.StartRecovery()
		return
	}

	//TODO: Handle msg from older views

	s.commitUpTo(primaryCommit)
	s.ResetHeartbeatTimer()
}

func (s *VR) CommitBroadcaster() {
	ticker := time.NewTicker(time.Millisecond * HEARTBEAT_INTERVAL_MS)
	for {
		<-ticker.C
		s.lock.RLock()
		if s.IsPrimary {
			for _, to := range s.GroupIDs {
				if to != s.Index {
					go s.Messenger.SendCommit(s.Index, to, s.ViewNumber, s.CommitNumber)
				}
			}
		}
		s.lock.RUnlock()
	}
}

//------------ Start of viewchange functions -------------
func (s *VR) HeartbeatTimeout() {
	<-(s.HeartbeatTimer).C
	if !(s.IsPrimary) {
		log.Println("Heartbeat timeout")
		s.viewChangeTimeout()
	}
}
func (s *VR) getTimerInterval() (interval time.Duration) {
	//Randomize the timeout by adding 0 to 100ms to avoid all replicas timing out at same time
	randomNum := rand.Int() % 100
	//log.Println("RandomNum = ", randomNum)
	return ((time.Millisecond * HEARTBEAT_INTERVAL_MS * VIEWCHANGE_TIMEOUT_MULTIPLE) + (time.Millisecond * time.Duration(randomNum)))
}

func (s *VR) ResetHeartbeatTimer() {
	if !(s.IsPrimary) {
		ret := (s.HeartbeatTimer).Reset(s.getTimerInterval())
		if ret == false {
			log.Println("Timer reset error!!")
		}
	}
}

func (s *VR) viewChangeRestartTimeout() {
	<-(s.ViewChangeRestartTimer).C
	//To allow view change to make progress in case stuck
	//Restart view change in next view
	log.Println("View change restart timeout")
	s.viewChangeTimeout()
}

func (s *VR) viewChangeTimeout() {
	s.lock.Lock()
	s.TriggerViewNum++
	s.lock.Unlock()
	go s.StartViewChange(s.TriggerViewNum, true)
}

func (s *VR) StartViewChange(newView int64, isTimerTriggered bool) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	//Initiate view change
	if s.Status == STATUS_NORMAL && newView > s.ViewNumber {
		s.Status = STATUS_VIEWCHANGE
		s.ViewChangeViewNum = newView
		s.NumOfStartViewChangeRecv++ //Increment for the one sent to ownself
		s.ViewChangeRestartTimer = time.NewTimer(s.getTimerInterval())
		go s.viewChangeRestartTimeout()

		log.Println("Start view change for new view number", newView)
		for _, to := range s.GroupIDs {
			if to != s.Index {
				go s.Messenger.SendStartViewChange(s.Index, to, s.ViewChangeViewNum)
			}
		}

		s.checkStartViewChangeQuorum()
	} else if s.Status == STATUS_VIEWCHANGE && newView > s.ViewChangeViewNum {
		//Case where a viewchange for an even later view is triggered
		s.restartViewChange(newView, isTimerTriggered)
		if s.ViewChangeRestartTimer != nil {
			log.Println("Reset viewchange timer")
			s.ViewChangeRestartTimer.Stop()
			s.ViewChangeRestartTimer = time.NewTimer(s.getTimerInterval())
			go s.viewChangeRestartTimeout()
		} else {
			log.Println("Error: No restart view change timer found!")
		}
	}
	return nil
}

func (s *VR) restartViewChange(newView int64, isTimerTriggered bool) (err error) {
	log.Println("Restarting viewchange to view number ", newView)
	s.resetViewChangeStates()
	s.ViewChangeViewNum = newView
	log.Println("Start view change for new view number", newView)
	for _, to := range s.GroupIDs {
		if to != s.Index {
			go s.Messenger.SendStartViewChange(s.Index, to, s.ViewChangeViewNum)
		}
	}
	if isTimerTriggered == true {
		s.NumOfStartViewChangeRecv++ //Increment for the one sent to ownself
	} else {
		s.NumOfStartViewChangeRecv = s.NumOfStartViewChangeRecv + 2 //Increment for the one sent to ownself & recv'd
	}
	s.checkStartViewChangeQuorum()

	return nil
}

func (s *VR) resetViewChangeStates() {
	//Reset all viewchange states
	s.ViewChangeViewNum = 0
	s.NumOfStartViewChangeRecv = 0
	s.NumOfDoViewChangeRecv = 0
	s.DoViewChangeSent = false
	s.DoViewChangeStatus.BestLogOpNum = -1
	s.DoViewChangeStatus.BestLogViewNum = -1
	s.DoViewChangeStatus.LargestCommitNum = -1
	s.DoViewChangeStatus.BestLogHeard = nil
}

func (s *VR) ReceiveStartViewChange(from int64, to int64, newView int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.Status == STATUS_NORMAL && newView > s.ViewNumber {
		log.Println("Hear startviewchange msg for first time")
		s.NumOfStartViewChangeRecv++
		s.HeartbeatTimer.Stop()
		go s.StartViewChange(newView, false)

	} else if (s.Status == STATUS_VIEWCHANGE) && (newView == s.ViewChangeViewNum) {
		log.Println("Hear startviewchange msg")
		s.NumOfStartViewChangeRecv++
	} else if (s.Status == STATUS_VIEWCHANGE) && (newView > s.ViewChangeViewNum) {
		go s.StartViewChange(newView, false)
	}

	s.checkStartViewChangeQuorum()
}

func (s *VR) ReceiveDoViewChange(from int64, to int64, recvNewView int64, recvOldView int64, recvLog []*LogEntry, recvOpNum int64, recvCommitNum int64) {

	log.Println("Recv DoViewChange msg")

	//Initiate view change if not in viewchange mode
	if s.Status == STATUS_NORMAL && recvNewView > s.ViewNumber {
		go s.StartViewChange(recvNewView, false)
	}

	//Increment number of DoViewChange msg recvd
	if s.Status == STATUS_VIEWCHANGE && s.ViewChangeViewNum == recvNewView {
		s.lock.Lock()
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
		s.lock.Unlock()
		if s.NumOfDoViewChangeRecv >= s.QuorumSize {
			s.StartView()
		}
	}
}

func (s *VR) StartView() {
	//New primary sets own states for new view
	s.lock.Lock()
	defer s.lock.Unlock()

	s.Log = s.DoViewChangeStatus.BestLogHeard
	s.ViewNumber = s.ViewChangeViewNum
	s.TriggerViewNum = s.ViewNumber
	s.OpNumber = s.DoViewChangeStatus.BestLogOpNum
	s.playLogUntil(s.DoViewChangeStatus.LargestCommitNum + 1)
	s.Status = STATUS_NORMAL
	s.IsPrimary = true
	s.ViewChangeRestartTimer.Stop()
	s.ViewChangeRestartTimer = nil
	s.resetViewChangeStates()
	for _, to := range s.GroupIDs {
		if to != s.Index {
			go s.Messenger.SendStartView(s.Index, to, s.ViewNumber, s.Log, s.OpNumber, s.CommitNumber)
		}
	}

	//TODO: Execute committed operations that have not previously been commited at this node i.e. commit up till recvCommitNum
	//TODO: Update client table and reply to clients if needed

	log.Println("Got quorum for doviewchange msg. Sent startview")
	log.Println("This is the NEW PRIMARY. New view number: ", s.ViewNumber)
}

func (s *VR) ReceiveStartView(from int64, to int64, recvNewView int64, recvLog []*LogEntry, recvOpNum int64, recvCommitNum int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.ViewNumber = recvNewView
	s.TriggerViewNum = recvNewView
	s.OpNumber = recvOpNum
	s.Log = recvLog
	s.Status = STATUS_NORMAL
	s.IsPrimary = false
	s.HeartbeatTimer = time.NewTimer(s.getTimerInterval())
	go s.HeartbeatTimeout()
	s.ViewChangeRestartTimer.Stop()
	s.ViewChangeRestartTimer = nil

	//TODO: Send prepareok for all non-committed operations
	//TODO: Execute committed operations that have not previously been commited at this node i.e. commit up till recvCommitNum
	//TODO: Update client table if needed

	s.resetViewChangeStates()

	log.Println("Received start view from new primary - node ", from)
	log.Println("New view number: ", s.ViewNumber)
}

func (s *VR) ReceiveStartRecovery() {
	go s.StartRecovery()
}

func (s *VR) checkStartViewChangeQuorum() {
	if s.NumOfStartViewChangeRecv >= s.QuorumSize && !(s.DoViewChangeSent) {
		i := s.ViewChangeViewNum % int64(len(s.GroupIDs)) //New leader index
		go s.Messenger.SendDoViewChange(s.Index, int64(i), s.ViewChangeViewNum, s.ViewNumber, s.Log, s.OpNumber, s.CommitNumber)
		log.Println("Got quorum for startviewchange msg. Sent doviewchange to new primary - node ", i)
		s.DoViewChangeSent = true
	}
}

//------------ End of viewchange functions -------------

//------------ Start of recovery functions -------------

func (s *VR) StartRecovery() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.Status != STATUS_RECOVERY {
		s.Status = STATUS_RECOVERY
		s.RecoveryStatus.RecoveryNonce = rand.Int63()
		s.IsPrimary = false //TODO: Check if it is correct to switch to 0 here
		s.RecoveryStatus.RecoveryRestartTimer = time.NewTimer(s.getTimerInterval())
		go s.RestartRecovery()

		log.Println("Start recovery protocol")
		for _, to := range s.GroupIDs {
			if to != s.Index {
				go s.Messenger.SendRecovery(s.Index, to, s.RecoveryStatus.RecoveryNonce, s.CommitNumber)
			}
		}
	}
}

func (s *VR) RestartRecovery() {
	<-(s.RecoveryStatus.RecoveryRestartTimer).C

	s.lock.Lock()
	defer s.lock.Unlock()

	s.resetRecoveryStatus()
	s.Status = STATUS_RECOVERY
	s.RecoveryStatus.RecoveryNonce = rand.Int63()
	s.IsPrimary = false //TODO: Check if it is correct to switch to 0 here
	s.RecoveryStatus.RecoveryRestartTimer = time.NewTimer(s.getTimerInterval())
	go s.RestartRecovery()

	log.Println("Restarting recovery protocol")
	for _, to := range s.GroupIDs {
		if to != s.Index {
			go s.Messenger.SendRecovery(s.Index, to, s.RecoveryStatus.RecoveryNonce, s.CommitNumber)
		}
	}
}

func (s *VR) ReceiveRecovery(from int64, to int64, nonce int64, lastCommitNum int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.Status == STATUS_NORMAL {
		log.Println("Received a recovery request from node ", from)
		if s.IsPrimary && s.Log != nil {
			go s.Messenger.SendRecoveryResponse(s.Index, from, s.ViewNumber, nonce, s.Log[lastCommitNum+1:], s.OpNumber, s.CommitNumber, s.IsPrimary)
		} else {
			go s.Messenger.SendRecoveryResponse(s.Index, from, s.ViewNumber, nonce, nil, -1, -1, s.IsPrimary)
		}
	}
}

func (s *VR) ReceiveRecoveryResponse(from int64, to int64, recvViewNum int64, recvNonce int64, recvLog []*LogEntry,
	recvOpNum int64, recvCommitNum int64, recvIsPrimary bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.Status == STATUS_RECOVERY {
		if recvNonce == s.RecoveryStatus.RecoveryNonce {
			log.Println("Received recovery response from node ", from)

			s.RecoveryStatus.NumOfRecoveryRspRecv++
			if recvViewNum > s.RecoveryStatus.LargestViewSeen {
				s.RecoveryStatus.LargestViewSeen = recvViewNum
			}
			if recvIsPrimary == true {
				log.Printf("Received recovery log from primary %v, length = %v, ViewNumber = %v, OpNumber = %v, CommitNumber = %v",
					from, len(recvLog), recvViewNum, recvOpNum, recvCommitNum)
				if s.RecoveryStatus.PrimaryViewNum < recvViewNum {
					s.RecoveryStatus.LogRecv = recvLog
					s.RecoveryStatus.PrimaryViewNum = recvViewNum
					s.RecoveryStatus.PrimaryId = from
					s.RecoveryStatus.PrimaryCommitNum = recvCommitNum
					s.RecoveryStatus.PrimaryOpNum = recvOpNum
					log.Println("Recovery log heard is best so far")
				}
			}

			if s.RecoveryStatus.NumOfRecoveryRspRecv >= s.QuorumSize {
				if s.RecoveryStatus.PrimaryViewNum == s.RecoveryStatus.LargestViewSeen {
					if s.Log == nil {
						s.Log = s.RecoveryStatus.LogRecv
					} else {
						// Cancel uncommitted operations
						for i := s.CommitNumber + 1; i < int64(len(s.Log)); i++ {
							if s.Log[i].ResultChan != nil {
								*s.Log[i].ResultChan <- errors.New("Error: View changed. Operation canceled.")
							}
						}
						// Append received updates to log
						s.Log = append(s.Log[:s.CommitNumber+1], s.RecoveryStatus.LogRecv...)
					}

					s.OpNumber = s.RecoveryStatus.PrimaryOpNum
					s.playLogUntil(s.RecoveryStatus.PrimaryCommitNum + 1)
					s.ViewNumber = s.RecoveryStatus.PrimaryViewNum

					s.Status = STATUS_NORMAL

					(s.RecoveryStatus.RecoveryRestartTimer).Stop()
					s.resetRecoveryStatus()
					log.Println("Recovery success. Updated to view number ", s.ViewNumber)
				}
			}
		} else {
			log.Println("Received an invalid recovery nonce")
		}
	} else {
		log.Println("Received a recovery response when not in recovery mode")
	}
}

func (s *VR) resetRecoveryStatus() {
	s.RecoveryStatus.LargestViewSeen = -1
	s.RecoveryStatus.NumOfRecoveryRspRecv = 0
	s.RecoveryStatus.PrimaryCommitNum = -1
	s.RecoveryStatus.PrimaryId = -1
	s.RecoveryStatus.PrimaryOpNum = -1
	s.RecoveryStatus.PrimaryViewNum = -2
	s.RecoveryStatus.RecoveryNonce = 0
	s.RecoveryStatus.LogRecv = nil
}

//------------ End of recovery functions -------------

// Internal functions, do not use lock
func (s *VR) playLogUntil(end int64) {
	log.Printf("Playing log until %v ... node CommitNumber = %v, OpNumber = %v", end, s.CommitNumber, s.OpNumber)

	if end <= s.CommitNumber {
		panic("Replaying log that is shorter than current log.")
	}
	if end == s.CommitNumber+1 {
		return
	}

	for i := s.CommitNumber + 1; i < end; i++ {
		logEntry := s.Log[i]
		log.Printf("s.CommitNumber = %v", s.CommitNumber)
		log.Printf("i = %v", i)
		log.Printf("log = %v", logEntry)
		log.Printf("Op = %v", *logEntry.Op)
		if tableEntry, ok := s.ClientTable[logEntry.Op.ClientID]; !ok || tableEntry.Op.RequestID < logEntry.Op.RequestID {
			s.ClientTable[logEntry.Op.ClientID] = &ClientTableEntry{Op: logEntry.Op, Processing: true}
		} else if !tableEntry.Processing {
			log.Printf("tableEntry: %v", *tableEntry)
			panic("ClientTable Entry contains Op that has finished processing. It should have been marked as commited.")
		} else if tableEntry.Op.RequestID > logEntry.Op.RequestID {
			log.Printf("tableEntry: %v", *tableEntry)
			panic("Smaller RequestID from the same client should not get appended to the log.")
		}
		s.CommitNumber = s.executeNextOp()
	}

	s.ViewNumber = s.Log[end-1].ViewNumber
	s.OpNumber = s.Log[end-1].OpNumber
}

func (s *VR) commitUpTo(primaryCommit int64) {
	for s.CommitNumber < primaryCommit && s.CommitNumber < s.OpNumber {
		s.CommitNumber = s.executeNextOp()
	}
}

// Execute the next Op that can be commited. Return the OpNum
func (s *VR) executeNextOp() (opNum int64) {
	num := s.CommitNumber + 1
	op := s.Log[num].Op
	s.Logger.Append(s.Log[num])
	s.ClientTable[op.ClientID].Result = s.Upcall(op)
	if s.Log[num].ResultChan != nil {
		*s.Log[num].ResultChan <- s.ClientTable[op.ClientID].Result
		close(*s.Log[num].ResultChan)
	}
	s.ClientTable[op.ClientID].Processing = false
	return num
}
