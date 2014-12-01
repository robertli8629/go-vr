package logging

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
)

type Log struct {
	View_number string
	Op_number   string
	Data        string
	//File
	//lock 
}

type LogStruct struct {
	Filename string
	Openfile *os.File
}

func NewLogStruct(openfile *os.File, filename string) *LogStruct {
	logStruct := LogStruct{Filename: filename, Openfile: openfile}
	return &logStruct
}

/*
sample for writing and reading from log, filename convention "logsX"
	l := logging.Log{"2","3","0-key-v"}

	logging.Write_to_log(l, filename)
	ls ,v, o := logging.Read_from_log()
*/

// write op_number, view_number, op_code, key, value
func WriteToLog(l Log, f *os.File) {

	text := ""
	text = text + l.View_number + " "
	text = text + l.Op_number + " "
	text = text + l.Data + " "
	text = text + "\n"

	if _, err := f.WriteString(text); err != nil {
		panic(err)
	}
}

// return a list of logs, the latest View_number and the latest Op_number
func ReadFromLog(filename string) (logs []string, view_number string, op_number string, commit_number string) {

	view_number = "-1"
	op_number = "-1"
	commit_number = "-1"

	file, err := os.Open(filename)
	if err != nil {
		return logs, view_number, op_number, commit_number
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	largestOpNumSeen := -1

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) >= 3 {
			view_number = fields[0]
			op_number = fields[1]
			//line = fields[2]
			opInt, _ := strconv.Atoi(op_number)
			if opInt > largestOpNumSeen {
				largestOpNumSeen = opInt
			}
		}
		logs = append(logs, line)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	commit_number = strconv.Itoa(largestOpNumSeen)

	return logs, view_number, op_number, commit_number
}

// replace log with string array logs
func ReplaceLogs(filename string, logs []string) {

	os.Remove(filename)
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	for l := range logs {
		line := logs[l]
		if _, err = f.WriteString(line + "\n"); err != nil {
			panic(err)
		}
	}

}
