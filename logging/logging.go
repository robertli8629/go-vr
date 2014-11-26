package logging

import (
	"bufio"
	"log"
	"os"
	"strings"
)

type Log struct {
	View_number string
	Op_number   string
	Data        string
}

type Log_struct struct {
	Filename    string
}

/*
sample for writing and reading from log, filename convention "logsX"
	l := logging.Log{"2","3","0-key-v", filename}

	logging.Write_to_log(l, filename)
	ls ,v, o := logging.Read_from_log()
*/

// write op_number, view_number, op_code, key, value
func Write_to_log(l Log, filename string) {
	//filename := "logs";
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	text := ""
	text = text + l.View_number + " "
	text = text + l.Op_number + " "
	text = text + l.Data + " "
	text = text + "\n"

	if _, err = f.WriteString(text); err != nil {
		panic(err)
	}
}

// return a list of logs, the latest View_number and the latest Op_number
func Read_from_log(filename string) (logs []string, view_number string, op_number string) {
	//filename := "logs";
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) >= 3 {
			view_number = fields[0]
			op_number = fields[1]
			line = fields[2]
		}
		logs = append(logs, line)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return logs, view_number, op_number
}
