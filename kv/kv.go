package kv

import (
	"strings"
	"sync"

	"github.com/robertli8629/cs244b_project/synchronous"
)

type KVStore struct {
	store  map[string]*string
	lock   *sync.RWMutex
	logger *synchronous.ReplicatedLog
}

// Enum for operation code
const PUT = 0
const DELETE = 1

func NewKVStore(logger *synchronous.ReplicatedLog) *KVStore {
	s := &KVStore{store: make(map[string]*string), lock: new(sync.RWMutex), logger: logger}
	if !logger.IsMaster {
		go s.updateFromLog()
	}
	return s
}

func (s *KVStore) updateFromLog() error {
	for {
		entry, err := s.logger.Messenger.ReceiveLogEntry()
		if err != nil {
			return err
		}
		switch entry.Op {
		case PUT:
			s.put(entry.Key, &entry.Value)
		case DELETE:
			s.delete(entry.Key)
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
	err = synchronous.AddLogEntry(s.logger, &synchronous.Entry{Op: PUT, Key: key, Value: *value})
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
	err = synchronous.AddLogEntry(s.logger, &synchronous.Entry{Op: DELETE, Key: key, Value: ""})
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
