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
This is