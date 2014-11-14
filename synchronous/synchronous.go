package synchronous

import (
	"errors"
)

type OpCode uint64

type Entry struct {
	Index uint64
	Op    OpCode
	Key   string
	Value string
}

type Messenger interface {
	SendLogEntry(uri string, entry *Entry) error
	ReceiveLogEntry() (*Entry, error)
}

type ReplicatedLog struct {
	IsMaster  bool
	Messenger Messenger
	log       []*Entry
	PeerUris   []string
}

func AddLogEntry(s *ReplicatedLog, entry *Entry) (err error) {
	if !s.IsMaster {
		return errors.New("Error: only the master can append to the log.")
	}

	for _, uri := range s.PeerUris {
		err := s.Messenger.SendLogEntry(uri, entry)
		if err != nil {
			break
		}
	}
	if err == nil {
		s.log = append(s.log, entry)
	}

	return err
}
