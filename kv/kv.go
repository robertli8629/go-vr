package kv

import (
	"encoding/json"
	//"log"
	"strings"
	"sync"
	"unicode"
	"strconv"
	
	"github.com/robertli8629/cs244b_project/vr"
)

type KVStore struct {
	store         map[string]*string
	lock          *sync.RWMutex
	requestNumber int64
	replication   *vr.VR
}

type Entry struct {
	Op    OpType
	Key   string
	Value string
}

// Enum for operation code
type OpType int64

const PUT = 0
const DELETE = 1

func NewKVStore(replication *vr.VR) *KVStore {
	store := KVStore{store: make(map[string]*string), lock: new(sync.RWMutex), requestNumber: 0, replication: replication}
	replication.RegisterUpcall(store.processMessage)
	return &store
}

func (s *KVStore) generateMessage(op OpType, key string, value string) (msg string) {
	b, _ := json.Marshal(&Entry{op, key, value})
	return string(b)
}

func (s *KVStore) processMessage(msg string) (result string) {
	var entry Entry
	err := json.Unmarshal([]byte(msg), &entry)
	if err != nil {
		return "Error: " + err.Error()
	}
	switch entry.Op {
	case PUT:
		s.put(entry.Key, &entry.Value)
		return "Success"
	case DELETE:
		s.delete(entry.Key)
		return "Success"
	default:
		panic(msg)
		return "Error: unknow message type: " + msg
	}
}

func (s *KVStore) ReplayLogs(logs []string) {
			
	for l := range logs {
		line := logs[l]
		f := func(c rune) bool {
			return !unicode.IsLetter(c) && !unicode.IsNumber(c)
		}
		fields := strings.FieldsFunc(line, f)

		op := fields[0]
		key := fields[1]
		value := fields[2]
		opid, err := strconv.Atoi(op)
		if err != nil {
			panic(err)
		}

		//log.Println(opid)
		//log.Println(key)
		//log.Println(value)
		switch opid {
			case PUT:
				s.put(key, &value)
			case DELETE:
				s.delete(key)
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
	err = s.replication.Request(s.generateMessage(PUT, key, *value), 0, s.requestNumber)
	s.lock.Unlock()
	if err == nil {
		s.put(key, value)
	}
	return err
}

func (s *KVStore) put(key string, value *string) {
	s.lock.Lock()
	s.store[key] = value
	s.lock.Unlock()
}

func (s *KVStore) Delete(key string) (err error) {
	s.lock.Lock()
	s.requestNumber++
	err = s.replication.Request(s.generateMessage(DELETE, key, ""), 0, s.requestNumber)
	s.lock.Unlock()
	if err == nil {
		s.delete(key)
	}
	return err
}

func (s *KVStore) delete(key string) {
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
