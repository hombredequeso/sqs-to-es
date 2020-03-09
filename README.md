# SQS to Elasticsearch using Akka Streams

Run up the Localstack aws testing framework, and an elasticsearch instance using docker-compose.
```
docker-compose up
```

## Run Locally
The following will startup the required infrastructure locally, add one message to the sqs queue, startup the program which will process the queue, adding a document to elasticsearch.
```
docker-compose up
aws --endpoint-url=http://localhost:4576 sqs send-message --queue-url http://localhost:4576/queue/mainqueue --message-body "{'id':123,'description':'entity123'}"
sbt run
curl localhost:9200/entity3/_search

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

```
aws --endpoint-url=http://localhost:4576 sqs list-queues

aws --endpoint-url=http://localhost:4576 sqs receive-message --queue-url http://localhost:4576/queue/testQueue --max-number-of-messages 10
```

For testing the main program, using localstack sqs
```
aws --endpoint-url=http://localhost:4576 sqs create-queue --queue-name mainqueue

```
