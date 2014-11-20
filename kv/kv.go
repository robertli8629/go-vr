package kv

import (
	"encoding/json"
	"strings"
	"sync"

	"github.com/robertli8629/cs244b_project/vr"
)

type KVStore struct {
	store  map[string]*string
	lock   *sync.RWMutex
	requestNumber int64
	replication *vr.VR
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
	return &KVStore{store: make(map[string]*string), lock: new(sync.RWMutex), requestNumber: 0, replication: replication}
}

func (s *KVStore) getMessage(op OpType, key string, value string) (msg string) {
	b, _ := json.Marshal(&Entry{op, key, value})
	return string(b)
}

func (s *KVStore) Get(key string) (value *string) {
	s.lock.RLock()
	value = s.store[key]
	s.lock.RUnlock()
	return value
}

func (s *KVStore) Put(key string, value *string) (err error) {
	s.requestNumber++
	err = s.replication.Request(s.getMessage(PUT, key, *value), 0, s.requestNumber)
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
	s.requestNumber++
	err = s.replication.Request(s.getMessage(DELETE, key, ""), 0, s.requestNumber)
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
