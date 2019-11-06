# SQS to Elasticsearch using Akka Streams

Run up the Localstack aws testing framework, and an elasticsearch instance using docker-compose.
```
docker-compose up
```

## Tests
Most tests are integration tests, using data stores and so forth. In particular they use localstack and Elasticsearch.

To run the tests:
```
docker-compose up
sbt test
```


## Localstack

Some helpful cli commands for testing with localstack:
https://lobster1234.github.io/2017/04/05/working-with-localstack-command-line/

``
aws --endpoint-url=http://localhost:4576 sqs list-queues

aws --endpoint-url=http://localhost:4576 sqs receive-message --queue-url http://localhost:4576/queue/testQueue --max-number-of-messages 10
``

