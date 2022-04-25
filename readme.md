# Introduce

This java spring boot project is a proxy for:

* send to message to kafka
* get message from mongoDB which used to prove data has been saved in database
* give a stream of data from a start timestamp to an end timestamp from kafka

#API

## Send message

    PUT 10.71.20.181:8080/api/v1/send?topic=test&message=message

return

    85e40b7f-05c3-4741-88ca-73dffcdbcb4d

## Get Data

Now you can use this message uuid to get message body:

    GET 10.71.20.181:8080/api/v1/get?key=85e40b7f-05c3-4741-88ca-73dffcdbcb4d

return

    {"_id":{"counter":7328952,"date":1641990378000,"machineIdentifier":4645115,"processIdentifier":6,"time":1641990378000,"timeSecond":1641990378,"timestamp":1641990378},"partition":0,"offset":5,"topic":"test","value":"d11","key":"85e40b7f-05c3-4741-88ca-73dffcdbcb4d"}

##  Get a stream of data from a start timestamp to an end timestamp

    GET 10.71.20.181:8080/api/v1/gets?topic=test&startTime=2022-01-12 17:40:00&endTime=2022-01-12 17:60:00

return

    ["message1","message2"]


# Configuration

You can find out the configuration file in:

    ./src/main/java/resources/application.yaml

you can set Kafka and MongoDB configuration in this file:
    
    # Kafka configuration
    kafka:
        producer:
            bootstrap-servers: 10.195.1.20:9094,10.195.1.19:9094,10.195.1.22:9094
            security-protocol: SASL_PLAINTEXT
            username: message-producer
            password: cerence_1
        consumer:
            bootstrap-servers: 10.195.1.20:9094,10.195.1.19:9094,10.195.1.22:9094
            security-protocol: SASL_PLAINTEXT
            username: message-reader
            password: cerence_1
            group-id: local-mongodb-sink
    
    # Mongodb configuration
    mongodb:
        user: root
        password: cerence_1
        host: dds-8vbe0445c6e12dd42.mongodb.zhangbei.rds.aliyuncs.com
        port: 3717
        # appName is used to compatibility with CosmosDB for mongodb, other mongodb server don't have an appName configuration
        appName:
        database: db-ali-test
        replicaSet: mgset-507875294
        ssl: false
        collectionName: backup

# How to use?

## Build Project

    mvn clean package

## Build Docker

    ./build.sh

## Run Docker

    docker run -d -p 8080:8080 beck/kafka-proxy:1.0


# Known Issues

* When sending a message need to check whether the topic exists 