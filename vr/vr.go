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
	SendPrepare(uri string, from int64, to int64, op *Operation, primaryView int64, primaryOp int64, primaryCommit int64) (err error)
	SendPrepareOK(uri string, from int64, to int64, backupView int64, backupOp int64) (err error)
	SendCommit(uri string, from int64, to int64, primaryView int64, primaryCommit int64) (err error)
	ReceivePrepare() (from int64, to int64, op *Operation, primaryView int64, primaryOp int64, primaryCommit int64, err error)
	ReceivePrepareOK() (from int64, to int64, backupView int64, backupOp int64, err error)
	ReceiveCommit() (from int64, to int64, primaryView int64, primaryCommit int64, err error)
	SendStartViewChange(uri string, from int64, to int64, newView int64) (err error)
	SendDoViewChange(uri string, from int64, to int64, newView int64, oldView int64, log []*LogEntry, opNum int64,
		commitNum int64) (err error)
	SendStartView(uri string, from int64, to int64, newView int64, log []*LogEntry, opNum int64, commitNum int64) (err error)
	SendRecovery(uri string, from int64, to int64, nonce int64, lastViewNum int64, lastOpNum int64) (err error)

	SendRecoveryResponse(uri string, from int64, to int64, viewNum int64, nonce int64, log []*LogEntry, opNum int64, commitNum int64, isPrimary bool) (err error)
	ReceiveStartViewChange() (from int64, to int64, newView int64, err error)
	ReceiveDoViewChange() (from int64, to int64, newView int64, oldView int64, log []*LogEntry, opNum int64, commitNum int64, err error)
	ReceiveStartView() (from int64, to int64, newView int64, log []*LogEntry, opNum int64, commitNum int64, err error)
	ReceiveRecovery() (from int64, to int64, nonce int64, lastViewNum int64, lastOpNum int64, err error)

	ReceiveRecoveryResponse() (from int64, to int64, viewNum int64, nonce int64, log []*LogEntry, opNum int64, commitNum int64, isPrimary bool, err error)
	ReceiveTestViewChange() (result bool, err error)
}

type Logger interface {
	Append(entry *LogEntry) (err error)
	ReadAll() (log []*LogEntry)
	ReplaceWith(log []*LogEntry) (err error)
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
}

type ClientTableEntry struct {
	Op         *Operation
	Processing bool
	ResultChan *chan interface{}
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
	GroupUris          map[int64]string
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

func NewVR(isPrimary bool, index int64, messenger Messenger, logger Logger, ids []int64, uris map[int64]string) (s *VR) {
	s = &VR{IsPrimary: isPrimary, Index: index, OpNumber: -1, CommitNumber: -1, Messenger: messenger, Logger: logger,
		GroupIDs: ids, GroupUris: uris, DoViewChangeSent: false, ViewChangeRestartTimer: nil}
	s.ClientTable = map[int64]*ClientTableEntry{}
	s.PrepareBallotTable = map[int64]map[int64]bool{}
	s.QuorumSize = int64(len(s.GroupIDs)/2 + 1)
	s.DoViewChangeStatus = DoViewChangeStore{BestLogOpNum: -1, LargestCommitNum: -1, BestLogViewNum: -1, BestLogHeard: nil}
	s.RecoveryStatus = RecoveryStore{LargestViewSeen: -1, PrimaryId: -1, PrimaryViewNum: -2, LogRecv: nil, PrimaryOpNum: -1, PrimaryCommitNum: -1}
	rand.Seed(time.Now().UTC().UnixNano() + s.Index)
	s.HeartbeatTimer = time.NewTimer(s.getTimerInterval())
	s.lock = &sync.RWMutex{}
	s.Status = STATUS_NORMAL
	return s
}

func (s *VR) InitializeService(callback func(op *Operation) (result interface{})) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.Upcall = callback
	s.Log = s.Logger.ReadAll()
	s.playLogUntil(int64(len(s.Log)))
	s.StartListeners()
}

func (s *VR) StartListeners() {
	go s.PrepareListener()
	go s.PrepareOKListener()
	go s.CommitListener()
	go s.CommitBroadcaster()
	go s.StartViewChangeListener()
	go s.DoViewChangeListener()
	go s.StartViewListener()
	go s.HeartbeatTimeout()
	go s.RecoveryListener()
	go s.RecoveryResponseListener()
	go s.TestViewChangeListener()
}

func (s *VR) RequestAsync(op *Operation) <-chan interface{} {
	log.Printf("Starting to replicate requestId=%v, message=%v from clientID=%v\n", op.RequestID, *op.Message, op.ClientID)
	s.lock.Lock()
	defer s.lock.Unlock()

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
	s.ClientTable[op.ClientID] = &ClientTableEntry{Op: op, Processing: true, ResultChan: &resultChan}
	s.PrepareBallotTable[s.OpNumber] = map[int64]bool{}
	s.Log = append(s.Log, &LogEntry{s.ViewNumber, s.OpNumber, op})

	for i, uri := range s.GroupUris {
		if int64(i) != s.Index {
			go s.Messenger.SendPrepare(uri, s.Index, int64(i), op, s.ViewNumber, s.OpNumber, s.CommitNumber)
		}
	}
	log.Println("Finished sending Prepare messages")

	return resultChan
}

func (s *VR) PrepareListener() {
	for {
		from, to, op, primaryView, primaryOp, primaryCommit, err := s.Messenger.ReceivePrepare()

		// Ignore parsing/transmission error, or out-of-date op
		if err != nil || primaryOp <= s.OpNumber {
			continue
		}

		if primaryView > s.ViewNumber {
			//If this node also thinks it is a prmary, will do recovery
			go s.StartRecovery()
			continue
		}

		if primaryOp > s.OpNumber+1 {
			go s.StartRecovery()
			continue
		}

		// Out-of-order message
		if primaryOp != s.OpNumber+1 {
			continue
		}

		//TODO: Handle msg from older views

		s.commitUpTo(primaryCommit)

		s.lock.Lock()
		s.OpNumber++
		s.ClientTable[op.ClientID] = &ClientTableEntry{Op: op, Processing: true}
		s.Log = append(s.Log, &LogEntry{s.ViewNumber, s.OpNumber, op})
		go s.Messenger.SendPrepareOK(s.GroupUris[from], to, from, s.ViewNumber, s.OpNumber)
		s.lock.Unlock()

		s.ResetHeartbeatTimer()
	}
}

func (s *VR) PrepareOKListener() {
	for {
		from, _, _, backupOpNum, err := s.Messenger.ReceivePrepareOK()
		if err != nil {
			continue
		}

		s.updateReceivedPrepareOK(from, backupOpNum)
		s.ResetHeartbeatTimer()
	}
}

func (s *VR) CommitListener() {
	for {
		_, _, primaryView, primaryCommit, err := s.Messenger.ReceiveCommit() //from, to, primaryView
		if err != nil {
			continue
		}

		if primaryView > s.ViewNumber {
			go s.StartRecovery()
			continue
		} else if primaryCommit >= s.CommitNumber+2 {
			//If fall behind in the same view by more than 2 commit numbers, recover.
			go s.StartRecovery()
			continue
		}

		//TODO: Handle msg from older views

		s.commitUpTo(primaryCommit)
		s.ResetHeartbeatTimer()
	}
}

func (s *VR) CommitBroadcaster() {
	ticker := time.NewTicker(time.Millisecond * HEARTBEAT_INTERVAL_MS)
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
	}
}

//------------ Start of viewchange functions -------------

func (s *VR) getTimerInterval() (interval time.Duration) {
	//Randomize the timeout by adding 0 to 100ms to avoid all replicas timing out at same time
	randomNum := rand.Int() % 100
	//log.Println("RandomNum = ", randomNum)
	return ((time.Millisecond * HEARTBEAT_INTERVAL_MS * VIEWCHANGE_TIMEOUT_MULTIPLE) + (time.Millisecond * time.Duration(randomNum)))
}

func (s *VR) HeartbeatTimeout() {
	<-(s.HeartbeatTimer).C
	if !(s.IsPrimary) {
		log.Println("Heartbeat timeout")
		s.ViewChangeTimeout()
	}
}

func (s *VR) ResetHeartbeatTimer() {
	if !(s.IsPrimary) {
		ret := (s.HeartbeatTimer).Reset(s.getTimerInterval())
		if ret == false {
			log.Println("Timer reset error!!")
		}
	}
}

func (s *VR) ViewChangeRestartTimeout() {
	<-(s.ViewChangeRestartTimer).C
	//To allow view change to make progress in case stuck
	//Restart view change in next view
	log.Println("View change restart timeout")
	s.ViewChangeTimeout()
}

func (s *VR) ViewChangeTimeout() {
	s.lock.Lock()
	s.TriggerViewNum++
	s.lock.Unlock()
	s.StartViewChange(s.TriggerViewNum, true)
}

func (s *VR) StartViewChange(newView int64, isTimerTriggered bool) (err error) {
	//Initiate view change
	if s.Status == STATUS_NORMAL && newView > s.ViewNumber {
		s.lock.Lock()
		s.Status = STATUS_VIEWCHANGE
		s.ViewChangeViewNum = newView
		s.NumOfStartViewChangeRecv++ //Increment for the one sent to ownself
		s.ViewChangeRestartTimer = time.NewTimer(s.getTimerInterval())
		go s.ViewChangeRestartTimeout()
		s.lock.Unlock()
		log.Println("Start view change for new view number", newView)
		for i, uri := range s.GroupUris {
			if int64(i) != s.Index {
				go s.Messenger.SendStartViewChange(uri, s.Index, int64(i), s.ViewChangeViewNum)
			}
		}

		s.CheckStartViewChangeQuorum()
	} else if s.Status == STATUS_VIEWCHANGE && newView > s.ViewChangeViewNum {
		//Case where a viewchange for an even later view is triggered
		s.RestartViewChange(newView, isTimerTriggered)
		if s.ViewChangeRestartTimer != nil {
			log.Println("Reset viewchange timer")
			s.ViewChangeRestartTimer.Stop()
			s.ViewChangeRestartTimer = time.NewTimer(s.getTimerInterval())
			go s.ViewChangeRestartTimeout()
		} else {
			log.Println("Error: No restart view change timer found!")
		}
	}
	return nil
}

func (s *VR) RestartViewChange(newView int64, isTimerTriggered bool) (err error) {
	log.Println("Restarting viewchange to view number ", newView)
	s.ResetViewChangeStates()
	s.lock.Lock()
	s.ViewChangeViewNum = newView
	s.lock.Unlock()
	log.Println("Start view change for new view number", newView)
	for i, uri := range s.GroupUris {
		if int64(i) != s.Index {
			go s.Messenger.SendStartViewChange(uri, s.Index, int64(i), s.ViewChangeViewNum)
		}
	}
	s.lock.Lock()
	if isTimerTriggered == true {
		s.NumOfStartViewChangeRecv++ //Increment for the one sent to ownself
	} else {
		s.NumOfStartViewChangeRecv = s.NumOfStartViewChangeRecv + 2 //Increment for the one sent to ownself & recv'd
	}
	s.lock.Unlock()
	s.CheckStartViewChangeQuorum()

	return nil
}

func (s *VR) ResetViewChangeStates() {
	//Reset all viewchange states
	s.lock.Lock()
	s.ViewChangeViewNum = 0
	s.NumOfStartViewChangeRecv = 0
	s.NumOfDoViewChangeRecv = 0
	s.DoViewChangeSent = false
	s.DoViewChangeStatus.BestLogOpNum = -1
	s.DoViewChangeStatus.BestLogViewNum = -1
	s.DoViewChangeStatus.LargestCommitNum = -1
	s.DoViewChangeStatus.BestLogHeard = nil
	s.lock.Unlock()
}

func (s *VR) StartViewChangeListener() {
	for {
		_, _, newView, err := s.Messenger.ReceiveStartViewChange()

		if err != nil {
			continue
		}
		if s.Status == STATUS_NORMAL && newView > s.ViewNumber {
			log.Println("Hear startviewchange msg for first time")
			s.lock.Lock()
			s.NumOfStartViewChangeRecv++
			s.lock.Unlock()
			s.StartViewChange(newView, false)
			s.HeartbeatTimer.Stop()
		} else if (s.Status == STATUS_VIEWCHANGE) && (newView == s.ViewChangeViewNum) {
			log.Println("Hear startviewchange msg")
			s.lock.Lock()
			s.NumOfStartViewChangeRecv++
			s.lock.Unlock()
		} else if (s.Status == STATUS_VIEWCHANGE) && (newView > s.ViewChangeViewNum) {
			s.StartViewChange(newView, false)
		}

		err = s.CheckStartViewChangeQuorum()
	}

}

func (s *VR) CheckStartViewChangeQuorum() (err error) {
	if s.NumOfStartViewChangeRecv >= s.QuorumSize && !(s.DoViewChangeSent) {
		i := s.ViewChangeViewNum % int64(len(s.GroupUris)) //New leader index
		uri := s.GroupUris[i]

		go s.Messenger.SendDoViewChange(uri, s.Index, int64(i), s.ViewChangeViewNum, s.ViewNumber, s.Log, s.OpNumber, s.CommitNumber)

		log.Println("Got quorum for startviewchange msg. Sent doviewchange to new primary - node ", i)
		s.lock.Lock()
		s.DoViewChangeSent = true
		s.lock.Unlock()
	}
	return nil
}

func (s *VR) CheckDoViewChangeQuorum() (err error) {
	if s.NumOfDoViewChangeRecv >= s.QuorumSize {
		err := s.StartView()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *VR) DoViewChangeListener() {
	for {
		from, _, recvNewView, recvOldView, recvLog, recvOpNum, recvCommitNum, err := s.Messenger.ReceiveDoViewChange()
		if err != nil {
			continue
		}
		log.Println("Recv DoViewChange msg")

		//Initiate view change if not in viewchange mode
		if s.Status == STATUS_NORMAL && recvNewView > s.ViewNumber {
			err = s.StartViewChange(recvNewView, false)
		}

		//Increment number of DoViewChange msg recvd
		if s.Status == STATUS_VIEWCHANGE && s.ViewChangeViewNum == recvNewView {
			s.NumOfDoViewChangeRecv++
			//Store logs if it is more updated than any logs previously heard
			//Check log view number and op number
			if (recvOldView > s.DoViewChangeStatus.BestLogViewNum) || (recvOldView == s.DoViewChangeStatus.BestLogViewNum && recvOpNum > s.DoViewChangeStatus.BestLogOpNum) {
				s.lock.Lock()
				s.DoViewChangeStatus.BestLogOpNum = recvOpNum
				s.DoViewChangeStatus.BestLogViewNum = recvOldView
				s.DoViewChangeStatus.BestLogHeard = recvLog
				s.lock.Unlock()
				log.Println("Hear better log from node ", from)
			}

			if recvCommitNum > s.DoViewChangeStatus.LargestCommitNum {
				s.lock.Lock()
				s.DoViewChangeStatus.LargestCommitNum = recvCommitNum
				s.lock.Unlock()
			}

			s.CheckDoViewChangeQuorum()
		}
	}
}

func (s *VR) StartView() (err error) {
	//New primary sets own states for new view
	s.lock.Lock()
	s.Log = s.DoViewChangeStatus.BestLogHeard
	s.ViewNumber = s.ViewChangeViewNum
	s.TriggerViewNum = s.ViewNumber
	s.OpNumber = s.DoViewChangeStatus.BestLogOpNum
	s.playLogUntil(s.DoViewChangeStatus.LargestCommitNum + 1)
	s.Status = STATUS_NORMAL
	s.IsPrimary = true
	s.ViewChangeRestartTimer.Stop()
	s.ViewChangeRestartTimer = nil
	s.lock.Unlock()
	s.ResetViewChangeStates()
	for i, uri := range s.GroupUris {
		if int64(i) != s.Index {
			go s.Messenger.SendStartView(uri, s.Index, int64(i), s.ViewNumber, s.Log, s.OpNumber, s.CommitNumber)
		}
	}

	//TODO: Execute committed operations that have not previously been commited at this node i.e. commit up till recvCommitNum
	//TODO: Update client table and reply to clients if needed

	log.Println("Got quorum for doviewchange msg. Sent startview")
	log.Println("This is the NEW PRIMARY. New view number: ", s.ViewNumber)

	return nil
}

func (s *VR) StartViewListener() {
	for {
		from, _, recvNewView, recvLog, recvOpNum, _, err := s.Messenger.ReceiveStartView()
		if err != nil {
			continue
		}
		s.lock.Lock()
		s.OpNumber = recvOpNum
		s.ViewNumber = recvNewView
		s.TriggerViewNum = recvNewView
		s.Log = recvLog
		s.Status = STATUS_NORMAL
		s.IsPrimary = false
		s.HeartbeatTimer = time.NewTimer(s.getTimerInterval())
		s.ViewChangeRestartTimer.Stop()
		s.ViewChangeRestartTimer = nil
		s.lock.Unlock()

		//TODO: Send prepareok for all non-committed operations
		//TODO: Execute committed operations that have not previously been commited at this node i.e. commit up till recvCommitNum
		//TODO: Update client table if needed

		s.ResetViewChangeStates()

		log.Println("Received start view from new primary - node ", from)
		log.Println("New view number: ", s.ViewNumber)
	}
}

func (s *VR) TestViewChangeListener() {
	for {
		_, err := s.Messenger.ReceiveTestViewChange()
		if err != nil {
			continue
		}

		s.StartRecovery()
	}
}

//------------ End of viewchange functions -------------

//------------ Start of recovery functions -------------

func (s *VR) StartRecovery() {
	if s.Status != STATUS_RECOVERY {
		s.lock.Lock()
		s.Status = STATUS_RECOVERY
		s.RecoveryStatus.RecoveryNonce = rand.Int63()
		s.IsPrimary = false //TODO: Check if it is correct to switch to 0 here
		s.RecoveryStatus.RecoveryRestartTimer = time.NewTimer(s.getTimerInterval())
		go s.RestartRecovery()
		s.lock.Unlock()
		log.Println("Start recovery protocol")
		for i, uri := range s.GroupUris {
			if int64(i) != s.Index {
				go s.Messenger.SendRecovery(uri, s.Index, int64(i), s.RecoveryStatus.RecoveryNonce, s.ViewNumber, s.OpNumber)
			}
		}
	}
}

func (s *VR) RestartRecovery() {
	<-(s.RecoveryStatus.RecoveryRestartTimer).C
	s.ResetRecoveryStatus()
	s.lock.Lock()
	s.Status = STATUS_RECOVERY
	s.RecoveryStatus.RecoveryNonce = rand.Int63()
	s.IsPrimary = false //TODO: Check if it is correct to switch to 0 here
	s.RecoveryStatus.RecoveryRestartTimer = time.NewTimer(s.getTimerInterval())
	go s.RestartRecovery()
	s.lock.Unlock()
	log.Println("Restarting recovery protocol")
	for i, uri := range s.GroupUris {
		if int64(i) != s.Index {
			go s.Messenger.SendRecovery(uri, s.Index, int64(i), s.RecoveryStatus.RecoveryNonce, s.ViewNumber, s.OpNumber)
		}
	}
}

func (s *VR) RecoveryListener() {
	for {
		from, _, nonce, _, _, err := s.Messenger.ReceiveRecovery()
		if err != nil {
			continue
		}
		if s.Status == STATUS_NORMAL {
			log.Println("Received a recovery request from node ", from)
			uri := s.GroupUris[from]
			if s.IsPrimary {
				// TODO: partial log recovery. maybe only in same view?
				s.Messenger.SendRecoveryResponse(uri, s.Index, from, s.ViewNumber, nonce, s.Log, s.OpNumber, s.CommitNumber, s.IsPrimary)
			} else {
				s.Messenger.SendRecoveryResponse(uri, s.Index, from, s.ViewNumber, nonce, nil, -1, -1, s.IsPrimary)
			}
		}
	}
}

func (s *VR) RecoveryResponseListener() {
	for {
		from, _, recvViewNum, recvNonce, recvLog, recvOpNum, recvCommitNum, recvIsPrimary, err := s.Messenger.ReceiveRecoveryResponse()
		if err != nil {
			continue
		}

		if s.Status == STATUS_RECOVERY {
			if recvNonce == s.RecoveryStatus.RecoveryNonce {
				log.Println("Received recovery response from node ", from)
				s.lock.Lock()
				s.RecoveryStatus.NumOfRecoveryRspRecv++
				if recvViewNum > s.RecoveryStatus.LargestViewSeen {
					s.RecoveryStatus.LargestViewSeen = recvViewNum
				}
				if recvIsPrimary == true {
					log.Println("Got a recovery log from node ", from)
					if s.RecoveryStatus.PrimaryViewNum < recvViewNum {
						s.RecoveryStatus.LogRecv = recvLog
						s.RecoveryStatus.PrimaryViewNum = recvViewNum
						s.RecoveryStatus.PrimaryId = from
						s.RecoveryStatus.PrimaryCommitNum = recvCommitNum
						s.RecoveryStatus.PrimaryOpNum = recvOpNum
						log.Println("Recovery log heard is best so far")
					}
				}
				s.lock.Unlock()
				if s.CheckRecoveryResponseQuorum() == true {
					if s.RecoveryStatus.PrimaryViewNum == s.RecoveryStatus.LargestViewSeen {
						s.lock.Lock()
						if s.ViewNumber != -1 || s.CommitNumber == -1 {
							s.Log = s.RecoveryStatus.LogRecv
						} else {
							//Appending to logs
							s.Log = append(s.Log, s.RecoveryStatus.LogRecv...)
						}

						s.ViewNumber = s.RecoveryStatus.PrimaryViewNum
						s.OpNumber = s.RecoveryStatus.PrimaryOpNum
						s.playLogUntil(s.RecoveryStatus.PrimaryCommitNum + 1)

						s.Status = STATUS_NORMAL
						s.lock.Unlock()

						(s.RecoveryStatus.RecoveryRestartTimer).Stop()
						s.ResetRecoveryStatus()
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
}

func (s *VR) CheckRecoveryResponseQuorum() (success bool) {
	if s.RecoveryStatus.NumOfRecoveryRspRecv >= s.QuorumSize {
		return true
	}
	return false
}

func (s *VR) ResetRecoveryStatus() {
	s.lock.Lock()
	s.RecoveryStatus.LargestViewSeen = -1
	s.RecoveryStatus.NumOfRecoveryRspRecv = 0
	s.RecoveryStatus.PrimaryCommitNum = -1
	s.RecoveryStatus.PrimaryId = -1
	s.RecoveryStatus.PrimaryOpNum = -1
	s.RecoveryStatus.PrimaryViewNum = -2
	s.RecoveryStatus.RecoveryNonce = 0
	s.RecoveryStatus.LogRecv = nil
	s.lock.Unlock()
}

//------------ End of recovery functions -------------

// Internal functions, do not use lock
func (s *VR) playLogUntil(end int64) {
	if end <= s.CommitNumber {
		panic("Replaying log that is shorter than current log.")
	}
	if end == s.CommitNumber+1 {
		return
	}

	for i := s.CommitNumber + 1; i < end; i++ {
		logEntry := s.Log[i]
		if tableEntry, ok := s.ClientTable[logEntry.Op.ClientID]; !ok || tableEntry.Op.RequestID < logEntry.Op.RequestID {
			s.ClientTable[logEntry.Op.ClientID] = &ClientTableEntry{Op: logEntry.Op, Processing: true}
		} else if !tableEntry.Processing {
			log.Printf("tableEntry: %v", *tableEntry)
			panic("ClientTable Entry that has finished processing should have been commited.")
		} else if tableEntry.Op.RequestID > logEntry.Op.RequestID {
			log.Printf("tableEntry: %v", *tableEntry)
			panic("Smaller RequestID from the same client should not get appended to the log.")
		}
		s.executeNextOp()
	}

	s.ViewNumber = s.Log[end-1].ViewNumber
	s.OpNumber = s.Log[end-1].OpNumber
}

func (s *VR) updateReceivedPrepareOK(from int64, backupOpNum int64) {
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
	return
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
	if s.ClientTable[op.ClientID].ResultChan != nil {
		*s.ClientTable[op.ClientID].ResultChan <- s.ClientTable[op.ClientID].Result
	}
	s.ClientTable[op.ClientID].Processing = false
	return num
}
