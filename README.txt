Go-VR

Running instructions:

After build it with command "go build". You can start 5 nodes with command 
./go-vr [0-4] [coldstart]. The first argument is a number from 0 to 4, which
specifies the index of the node. The node with index 0 will be the master
by default. The second argument "coldstart" is optional. When it is specified,
The node will create a new log file to write on instead of append to the end
of the old log file. 


Folder details:

kv:
	module of key-value store
logging:
	module of logs
server:
	module of server that uses REST api
starter:
	Initializer
synchronous:
	module of the json messengers
vr:
	module of view-stamp replication implementation
tests:
	folder that contains different test cases
 
