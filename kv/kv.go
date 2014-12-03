package kv

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/robertli8629/go-vr/logging"
	"github.com/robertli8629/go-vr/vr"
)

type KVStore struct {
	id            int64
	store         map[string]*string
	lock          *sync.RWMutex
	requestNumber int64
	replication   *vr.VR
}

type Message struct {
	Op    OpType
	Key   string
	Value string
}

// Enum for operation code
type OpType int64

const PUT = 0
const DELETE = 1

func NewKVStore(id int64, replication *vr.VR) *KVStore {
	store := KVStore{id: id, store: make(map[string]*string), lock: new(sync.RWMutex), requestNumber: 0, replication: replication}
	replication.RegisterUpcall(store.processMessage)
	replication.RegisterReplayLogUpcall(store.ReplayLogs)
	return &store
}

func (s *KVStore) generateMessage(op OpType, key string, value string) (msg string) {
	b, _ := json.Marshal(&Message{op, key, value})
	return string(b)
}

func (s *KVStore) processMessage(msg string) (result string) {
	var message Message
	err := json.Unmarshal([]byte(msg), &message)
	if err != nil {
		return "Error: " + err.Error()
	}
	switch message.Op {
	case PUT:
		s.put(message.Key, &message.Value)
		return "Success"
	case DELETE:
		log.Println("deleting: " + message.Key)
		s.delete(message.Key)
		return "Success"
	default:
		panic(msg)
		return "Error: unknow message type: " + msg
	}
}

func (s *KVStore) ReplayLogs(logs []string) {
	log.Println("Replaying logs....")
	for l := range logs {
		line := logs[l]
		f := func(c rune) bool {
			if c == '/' {
				return false
			}
			return !unicode.IsLetter(c) && !unicode.IsNumber(c)
		}
		fields := strings.FieldsFunc(line, f)

		op := fields[2]
		key := fields[3]
		value := fields[4]
		opid, err := strconv.Atoi(op)
		if err != nil {
			panic(err)
		}

		switch opid {
		case PUT:
			s.replayPut(key, &value)
		case DELETE:
			s.replayDelete(key)
		default:
			panic(op)
			return
		}
	}
}

func (s *KVStore) Get(key string) (value *string) {
	s.lock.RLock()
	value = s.store[key]
	s.lock.RUnlock()
	return value
}

func (s *KVStore) Put(key string, value *string) (err error) {
	s.lock.Lock()
	s.requestNumber++
	err = s.replication.Request(s.generateMessage(PUT, key, *value), s.id, s.requestNumber)
	s.lock.Unlock()
	if err == nil {
		s.put(key, value)
	}
	return err
}

func (s *KVStore) put(key string, value *string) {
	s.lock.Lock()
	s.store[key] = value
	// add to log
	text := ""
	text = text + "0-" + key + "-" + *value
	l := logging.Log{strconv.FormatInt(s.replication.ViewNumber, 10), strconv.FormatInt(s.replication.OpNumber, 10), text}
	logging.WriteToLog(l, s.replication.LogStruct.Filename)
	s.lock.Unlock()
}

func (s *KVStore) replayPut(key string, value *string) {
	s.lock.Lock()
	s.store[key] = value
	s.lock.Unlock()
}

func (s *KVStore) Delete(key string) (err error) {
	s.lock.Lock()
	s.requestNumber++
	err = s.replication.Request(s.generateMessage(DELETE, key, ""), s.id, s.requestNumber)
	s.lock.Unlock()
	if err == nil {
		s.delete(key)
	}
	return err
}

func (s *KVStore) delete(key string) {
	s.lock.Lock()
	delete(s.store, key)
	// add to log
	text := ""
	text = text + "1-" + key + "-0"
	l := logging.Log{strconv.FormatInt(s.replication.ViewNumber, 10), strconv.FormatInt(s.replication.OpNumber, 10), text}
	logging.WriteToLog(l, s.replication.LogStruct.Filename)
	s.lock.Unlock()
}

func (s *KVStore) replayDelete(key string) {
	s.lock.Lock()
	delete(s.store, key)
	s.lock.Unlock()
}

func (s *KVStore) List(prefix string) (list []string) {
	list = []string{}
	s.lock.Lock()
	for key, _ := range s.store {
		if strings.HasPrefix(key, prefix) {
			list = append(list, key)
		}
	}
	s.lock.Unlock()
	return list
}
