package kv

import (
	"sync"
	"strings"
)

var store = map[string]*string{}

var lock = sync.RWMutex{}

func Get(key string) (value *string) {
	lock.RLock()
	value = store[key]
	lock.RUnlock()
	return value
}

func Put(key string, value string) {
	lock.Lock()
	store[key] = &value
	lock.Unlock()
}

func Delete(key string) {
	lock.Lock()
	delete(store, key)
	lock.Unlock()
}

func List(prefix string) (list []string) {
	list = []string{} 
	lock.Lock()
	for key, _ := range store {		
		if strings.HasPrefix(key, prefix) {
			list = append(list, key)
		}
	}
	lock.Unlock()
	return list
}
