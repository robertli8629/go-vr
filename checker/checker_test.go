package checker

import (
	"strconv"
	"testing"

	"github.com/robertli8629/cs244b_project/vr"
)

const N_INSTANCES = 5

func TestVR(t *testing.T) {
	id := make([]int64, N_INSTANCES)
	port := make([]int, N_INSTANCES)
	uri := map[int64]string{}
	for i := 0; i < N_INSTANCES; i++ {
		id[i] = int64(i)
		port[i] = 10340 + i
		uri[id[i]] = "http://127.0.0.1:" + strconv.Itoa(port[i])
	}

	messenger := make([]*vr.JsonMessenger, N_INSTANCES)
	replication := make([]*vr.VR, N_INSTANCES)
	for i := 0; i < N_INSTANCES; i++ {
		messenger[i] = vr.NewJsonMessenger(strconv.Itoa(port[i]))
		replication[i] = vr.NewVR(i == 0, int64(i), messenger[i], id, uri)
	}

	// Do not exit
	<-make(chan interface{})
}
