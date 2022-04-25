#!/bin/bash

set -e

echo "START kafka proxy"

java -jar -Xms6g -Xmx6g  -XX:NewRatio=3 -Dlog4j.configurationFile=./log4j2.xml ./app.jar

echo "END Running app on `date`"