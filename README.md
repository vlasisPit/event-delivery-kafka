# Project structure
```
event-delivery-kafka
└───api
│      └───server       : contains routes supported by the REST API and the REST endpoint to accept events and send them to a Kafka topic 
│      └───utils        : helpers to create success and error responses
│      └───app.go       : class that accepts application properties and instantiate all necessary components, like REST server, kafka Producer, Kafka consumers and exponential backoff component
│      └───app_test.go  : contains E2E tests. Run and verify the basic end to end scenarios
|
└───config
│      └───.env         : Environment variables loaded during application start.
│   
└───delivery
│      └───destinations : Package that contains classes to mock destinations behaviour. Mock destinations with failures, delays, successes and both successes and failures to verify retry functionality 
|
└───kafka
│      └───backoff      : exponential backoff algorithm with max retries 3.  
│      └───components   : kafka components like topics, consumers and producers
│      └───processors   : Struct to wrap the action that should be executed by kafka consumer
|
└───models              : Models like event and kafka message
|
└───docker-compose.yaml : Run docker-compose up to run spin up a kafka container to run the application in dev mode. Also necessary to run end to end tests.
|
└───Makefile        : Makefile to run the tests and start the application.  
```

# Documentation and Technical Decisions
This is a system that receives events from multiple users and delivers (broadcast) them in multiple destinations. It consists of a REST endpoint that accepts ingested events and produce them in a Kafka topic. Then kafka consumers read those events and send them to destination. Event should have the following structure `struct { UserID string; Payload string }`.  
The following requirements are met   
1. **Durability** : Every event that has been produced to a Kafka topic, it remains in the system for 24 hours. When this time duration passes, then the event is deleted automatically. To achieve this, topic's property `log.retention.hours` is set with value 24. Check `api/app.go:100`.
2. **At least-once delivery** : At least-once delivery of events to a destination means that the event should be delivered to destination at-least one time. More deliveries of the same event is allowed. This is achieved by committing consumer offset manually when all attempts to send the event to the destinations have been completed. For this reason `FetchMessage` is used to retrieve a message from the topic, then backoff mechanism runs until the maxRetries limit is reached and then the offset is committed with `CommitMessages`. Check `kafka/components/consumer.go:71`.
3. **At least-once from producer side** : Producer waits an ack from all kafka nodes. If an ack is not received, then producer retries to send the message to kafka. Check `api/app.go:46`.  
4. **Retry backoff and limit** : External library `github.com/cenkalti/backoff/v4` used. To send the event to a destination, an exponential backoff strategy is used with 3 max retries. If all retries fail, then the offset is committed and the consumer will read the next message in topic. Custom values are passed in backoff strategy to run sooner retry requests. Check `api/app.go:128`.
5. **Maintaining order** : Events of the same user should always be delivered in the order the system received them. Kafka supports message ordering across the same partition. So, to ensure this requirement, every message with the same ID should be delivered to the same partition. So, `Murmur2Balancer` was used as partitioner method to send the messages to kafka topic. According `Murmur2Balancer` documentation, it ensures that messages with the same key are routed to the same partition. Check `api/app.go:43`.
6. **Delivery isolation** : To ensure that delays or failures with the event delivery of a single destination will not affect ingestion or delivery to other destinations, one consumer per destination is created to deliver messages to specific destination. Those consumers should have **different** `groupId` to keep track of the offsets committed per destination (check `api/app.go:56`). It is possible to use more consumers per destination, but they need to have the same `groupId`. In that way, consumers for each destination read offsets from the same topic independently. Event delivery is not affected by failures of a specific userId, because every consumer uses a backoff algorithm with retries to send the message and then (if all retries failed) proceeds to the next event.

# Makefile
The following commands are supported
1. `make test_all` : Run all test cases
2. `make go_run` : Start locally the server to receive events from users. Start all necessary components to support event delivery to the destinations. All kafka logs are shown in terminal output.
3. `make go_run_without_kafka_internal_logs` : Same as command 2, but all internal kafka logs are filtered out. Easy to check logs related to exponential backoff ,retry and success/failure of each destination. Also, a log related to topic and partition is shown when the offset is committed. 

# Testing
To run end-to-end tests and test the basic functionality of the system, library `github.com/testcontainers/testcontainers-go` was used to spin up a kafka docker container to produce and consume messages from. Kafka docker image used does not contain a Zookeeper instance, because it uses Kafka Raft (also known as KRaft), a new consensus protocol introduced to replace Zookeeper.
End-to-end tests are in `api/app_test.go` file and to execute them, you need to run `go test -v ./api`. Those tests use a mock destination (called `DestinationMock`) and for each test a `func` is passed as argument in order to simulate the following scenarios
1. Test a destination that fails repeatedly
2. Test a destination that fails repeatedly and another that always successes for each request
3. Test a destination that fails repeatedly with timeouts
4. Test a destination that fails for the first request and successes for the next one.

Assertions are based on the number of times that the consumer makes a request to the destination and the userId received, alongside with the status of the request.

# Execution
In folder `delivery/destinations/mocks`, there are some mock destinations to run and verify the system. This folder contains a destination that always fails, a destination that always succeed, a destination that fails because of a delay and a destination that both fails and succeed randomly.
So, to run the system, first we need to spawn a kafka docker container using `docker-compose.yml`. Then the system must be started (choose command 2 or 3 from Makefile) and then make a request to the server to send an event. For example

```
curl -X PUT -H "Content-Type: application/json" -d '{"user_id": "user_test_1", "payload": "event click !!!!"}' localhost:8080/events
```

This curl request has as a side effect an event to be produced to Kafka. Then kafka consumers (5 consumers - 1 for each destination) will try to send the event to each of the 5 destinations. Below is the output of the system.

```
 1 - $ make go_run_without_kafka_internal_logs
 2 - go run main.go | grep -v "reader"
 3 - 2022/08/10 18:08:52 failed to send message: snowflake: Can't receive event for userId user_test_1  for key user_test_1
 4 - custom exponential backoff runner: error: snowflake: Can't receive event for userId user_test_1 , retrying after 0.15284835 seconds

 5 - 2022/08/10 18:08:52 failed to send message: postgres: Can't receive event for userId user_test_1  for key user_test_1
 6 - custom exponential backoff runner: error: postgres: Can't receive event for userId user_test_1 , retrying after 0.224821385 seconds

 7 - 2022/08/10 18:08:52 bigquery: Received event successfully for userId user_test_1
 8 - 2022/08/10 18:08:52 failed to send message: redshift: Can't receive event for userId user_test_1  for key user_test_1
 9 - custom exponential backoff runner: error: redshift: Can't receive event for userId user_test_1 , retrying after 0.34282698 seconds

10 -    topic: event-log
11 -            partition 9: 2
12 - 2022/08/10 18:08:52 failed to send message: snowflake: Can't receive event for userId user_test_1  for key user_test_1
13 - custom exponential backoff runner: error: snowflake: Can't receive event for userId user_test_1 , retrying after 0.492649716 seconds

14 - 2022/08/10 18:08:52 failed to send message: postgres: Can't receive event for userId user_test_1  for key user_test_1
15 - custom exponential backoff runner: error: postgres: Can't receive event for userId user_test_1 , retrying after 0.345064947 seconds

16 - 2022/08/10 18:08:53 redshift: Received event successfully for userId user_test_1
17 -    topic: event-log
18 -            partition 9: 2
19 - 2022/08/10 18:08:53 failed to send message: postgres: Can't receive event for userId user_test_1  for key user_test_1
20 - custom exponential backoff runner: error: postgres: Can't receive event for userId user_test_1 , retrying after 0.452061151 seconds

21 - 2022/08/10 18:08:53 failed to send message: snowflake: Can't receive event for userId user_test_1  for key user_test_1
22 - custom exponential backoff runner: error: snowflake: Can't receive event for userId user_test_1 , retrying after 0.469439037 seconds

23 - 2022/08/10 18:08:53 failed to send message: azureDataLakeMock : timed out for key user_test_1
24 - custom exponential backoff runner: error: azureDataLakeMock : timed out, retrying after 0.223806511 seconds

25 - 2022/08/10 18:08:53 failed to send message: postgres: Can't receive event for userId user_test_1  for key user_test_1
26 - 2022/08/10 18:08:53 failed to run operation using exponential backoff strategy: postgres: Can't receive event for userId user_test_1  for key user_test_1
27 -    topic: event-log
28 -            partition 9: 2
29 - 2022/08/10 18:08:53 failed to send message: snowflake: Can't receive event for userId user_test_1  for key user_test_1
30 - 2022/08/10 18:08:53 failed to run operation using exponential backoff strategy: snowflake: Can't receive event for userId user_test_1  for key user_test_1
31 -    topic: event-log
32 -            partition 9: 2
33 - 2022/08/10 18:08:54 failed to send message: azureDataLakeMock : timed out for key user_test_1
34 - custom exponential backoff runner: error: azureDataLakeMock : timed out, retrying after 0.228745659 seconds

35 - 2022/08/10 18:08:56 failed to send message: azureDataLakeMock : timed out for key user_test_1
36 - custom exponential backoff runner: error: azureDataLakeMock : timed out, retrying after 0.415574068 seconds

37 - 2022/08/10 18:08:57 failed to send message: azureDataLakeMock : timed out for key user_test_1
38 - 2022/08/10 18:08:57 failed to run operation using exponential backoff strategy: azureDataLakeMock : timed out for key user_test_1
39 -    topic: event-log
40 -            partition 9: 2
41 - 2022/08/10 18:09:19 Consumer reader for groupId event-delivery-kafka-snowflake closed
42 - 2022/08/10 18:09:19 Consumer reader for groupId event-delivery-kafka-redshift closed
43 - 2022/08/10 18:09:19 Consumer reader for groupId event-delivery-kafka-postgres closed
44 - 2022/08/10 18:09:19 Consumer reader for groupId event-delivery-kafka-azureDataLakeMock closed
45 - 2022/08/10 18:09:19 Consumer reader for groupId event-delivery-kafka-bigquery closed
46 - exit status 1
```

For destination `postgres` that fails repeatedly, backoff mechanism run 3 retries to deliver the message with no success. Check lines 6,15 and 20. At line 26, there is the last log about `postgres` from exponential backoff component. If you run the application with full log output, you will see that lines 27-28 is about committing the offset from `postgres` consumer after all 3 retries have failed.  
The same exactly outcome is happening with `snowflake`, which fails repeatedly. Check lines 4,13,22 and lines 31-32 about committing the offset from `snowflake` consumer.  
Destination `azureDataLakeMock` fails repeatedly because of a long delay. Check lines 24, 34 and 36 about retrying to deliver the event(timeout error message). Commit offset from `azureDataLakeMock` consumer in lines 39-40.  
Destination `bigquery` succeed for every request. So, at line 7 there is a success log without any retry. Consumer `bigquery` commits offset at lines 10-11.  
Destination `redshift` fails for the first request (line 9) but succeed on the second request (line 16). Consumer `redshift` commits offset at lines 17-18.  
At lines 41-46, all consumers closed after stopping the server, using `signal.Notify` to trigger closing. Check `kafka/components/consumer.go:49` for more details.

# Final Notes
The existing implementation uses a running thread to serve a kafka consumer. The application needs one thread per destination. If we need to deliver messages to a huge number of destinations, then it is not a good solution to use a new thread for each.  
A solution is to pass a subset of the destination on `app.go` class `api/app.go:26` and start more than one instances of this application. This will need some changes in the current implementation to pass Destinations on a property file or as arguments.