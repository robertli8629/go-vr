package vr

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
)

type JsonLogger struct {
	logfile *os.File
	logs chan *LogEntry
	encoder *json.Encoder
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

	logger = &JsonLogger{logfile: logfile, logs: make(chan *LogEntry, 1000), encoder: json.NewEncoder(logfile)}
	go logger.writer()

	return logger
}

func (s *JsonLogger) Append(entry *LogEntry) (err error) {
	s.logs <- entry
	return nil
}

func (s *JsonLogger) ReadAll() (logs []*LogEntry) {
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

func (s *JsonLogger) writer() {
	for {
		entry := <- s.logs
		s.encoder.Encode(entry)
	}
}