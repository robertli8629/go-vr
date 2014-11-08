curl -X PUT -i -d '{"value" : "ccc"}' http://127.0.0.1:8080/object/aaa
curl -X PUT -i -d '{"value" : "ccc"}' http://127.0.0.1:8080/object/aaa/bbb
curl -X GET -i http://127.0.0.1:8080/object/aaa
