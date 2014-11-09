package kv

import (
	"strings"
	"sync"
)

type KVStore struct {
	store map[string]*string
	lock  *sync.RWMutex
}

func NewKVStore() *KVStore {
	return &KVStore{store: make(map[string]*string), lock: new(sync.RWMutex)}
}

func (s *KVStore) Get(key string) (value *string) {
	s.lock.RLock()
	value = s.store[key]
	s.lock.RUnlock()
	return value
}

func (s *KVStore) Put(key string, value *string) {
	s.lock.Lock()
	s.store[key] = value
	s.lock.Unlock()
}

func (s *KVStore) Delete(key string) {
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
