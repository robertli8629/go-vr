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
	writer  *bufio.Writer
	encoder *json.Encoder
	lock    *sync.Mutex
}

func NewJsonLogger(filename *string, appendOnly bool) (logger *JsonLogger) {
	_, err := os.Stat(*filename)

	mode := os.O_RDWR
	if err == nil {
		mode = mode | os.O_APPEND
	} else {
		mode = mode | os.O_CREATE
	}

	log.Println("Opening log file...")
	logfile, err := os.OpenFile(*filename, mode, 0660)
	if err != nil {
		panic(err)
	}
	if !appendOnly {
		err = logfile.Truncate(0)
		if err != nil {
			panic(err)
		}
	}

	writer := bufio.NewWriter(logfile)
	return &JsonLogger{logfile: logfile, writer: writer, encoder: json.NewEncoder(writer), lock: new(sync.Mutex)}
}

func (s *JsonLogger) Append(entry *LogEntry) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	err = s.encoder.Encode(entry)
	go s.writer.Flush()
	return err
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
