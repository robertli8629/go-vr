#!/bin/bash
for i in {1..500}
do
   curl -X PUT -i -d '{"value" : "bbb"}' http://127.0.0.1:8080/object/aaa
   curl -X DELETE -i http://127.0.0.1:8080/object/aaa
done

