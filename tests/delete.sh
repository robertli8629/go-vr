# delete testing
curl -X PUT -i -d '{"value" : "bbb"}' http://127.0.0.1:8080/object/aaa
curl -X PUT -i -d '{"value" : "ddd"}' http://127.0.0.1:8080/object/aaa/ccc
curl -X GET -i http://127.0.0.1:8080/object/aaa
curl -X GET -i http://127.0.0.1:8080/object/aaa/ccc
curl -X DELETE -i http://127.0.0.1:8080/object/aaa
curl -X DELETE -i http://127.0.0.1:8080/object/aaa/ccc
curl -X GET -i http://127.0.0.1:8080/object/aaa
curl -X GET -i http://127.0.0.1:8080/object/aaa/ccc
