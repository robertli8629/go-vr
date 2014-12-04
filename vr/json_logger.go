package vr

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"sync"
)

type JsonLogger struct {
	logfile *os.File
	encoder *json.Encoder
	lock    *sync.Mutex
}

func NewJsonLogger(filename *string, appendOnly bool) (logger *JsonLogger) {
	_, err := os.Stat(*filename)

	mode := os.O_RDWR
	if appendOnly && err == nil {
		mode = mode | os.O_APPEND
	} else {
		mode = mode | os.O_CREATE
	}

	log.Println("Opening log file...")
	logfile, err := os.OpenFile(*filename, mode, 0660)
	if err != nil {
		panic(err)
	}

	return &JsonLogger{logfile: logfile, encoder: json.NewEncoder(bufio.NewWriter(logfile)), lock: new(sync.Mutex)}
}

func (s *JsonLogger) Append(entry *LogEntry) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.encoder.Encode(entry)
}

func (s *JsonLogger) ReadAll() (logs []*LogEntry) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Rewind to the beginning
	s.logfile.Seek(0, 0)

	decoder := json.NewDecoder(bufio.NewReader(s.logfile))
	for {
		entry := new(LogEntry)
		err := decoder.Decode(entry)
		if err == nil {
			logs = append(logs, entry)
		} else {
			log.Printf("Finished reading %v entries from log file: %v.\n", len(logs), err)
			break
		}
	}

	// Forward to the end
	s.logfile.Seek(0, 2)

	return logs
}

func (s *JsonLogger) ReplaceWith(log []*LogEntry) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	err = s.logfile.Truncate(0)
	if err != nil {
		return err
	}

	for _, l := range log {
		err = s.encoder.Encode(l)
		if err != nil {
			return
		}
	}

	return nil
}
