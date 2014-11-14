curl -X PUT -i -d '{"value" : "bbb"}' http://127.0.0.1:8080/object/aaa
curl -X PUT -i -d '{"value" : "ddd"}' http://127.0.0.1:8080/object/aaa/ccc
curl -X GET -i http://127.0.0.1:8080/object/aaa
curl -X GET -i http://127.0.0.1:8100/object/aaa/ccc
curl -X PUT -i -d '{"value" : "eee"}' http://127.0.0.1:8090/object/aaa/ccc
