package kv

import (
	"encoding/json"
	"log"
	"sync"

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
	store := KVStore{id: id, store: make(map[string]*string), lock: new(sync.RWMutex), requestNumber: -1, replication: replication}
	replication.InitializeService(store.processMessage)
	return &store
}

func (s *KVStore) generateMessage(op OpType, key string, value string) (msg *string) {
	b, _ := json.Marshal(&Message{op, key, value})
	str := string(b)
	return &str
}

func (s *KVStore) processMessage(op *vr.Operation) (result interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.requestNumber = op.RequestID

	var message Message
	err := json.Unmarshal([]byte(*op.Message), &message)
	if err != nil {
		return err
	}

	switch message.Op {
	case PUT:
		s.put(message.Key, &message.Value)
	case DELETE:
		s.delete(message.Key)
	default:
		panic(op)
	}

	return nil
}

func (s *KVStore) Get(key string) (value *string) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.store[key]
}

func (s *KVStore) Put(key string, value *string) (err error) {
	s.lock.Lock()
	s.requestNumber++
	op := &vr.Operation{s.id, s.requestNumber, s.generateMessage(PUT, key, *value)}
	resultChan := s.replication.RequestAsync(op)
	s.lock.Unlock()
	log.Printf("Waiting for result: Request %v - Put %v %v\n", s.requestNumber, key, *value)
	return receiveError(resultChan)
}

func (s *KVStore) put(key string, value *string) {
	s.store[key] = value
}

func (s *KVStore) Delete(key string) (err error) {
	s.lock.Lock()
	s.requestNumber++
	op := &vr.Operation{s.id, s.requestNumber, s.generateMessage(DELETE, key, "")}
	resultChan := s.replication.RequestAsync(op)
	s.lock.Unlock()
	log.Printf("Waiting for result: Request %v - Delete %v\n", s.requestNumber, key)
	return receiveError(resultChan)
}

func (s *KVStore) delete(key string) {
	delete(s.store, key)
}

func receiveError(resultChan <-chan interface{}) (err error) {
	result, ok := <-resultChan
	if !ok {
		panic("Unexpected: Channel closed.")
	}
	if result != nil {
		if err, ok = result.(error); !ok {
			panic(result)
		}
	}
	return err
}
