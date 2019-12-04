package com.hombredequeso.sqstoes.test

import akka.{Done, NotUsed}
import akka.stream._
import akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchFlow
import akka.stream.alpakka.elasticsearch.{ElasticsearchWriteSettings, RetryAtFixedRate, _}
import akka.stream.alpakka.sqs.{MessageAction, SqsSourceSettings}
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import com.hombredequeso.sqstoes.SqsService
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import software.amazon.awssdk.services.sqs.model.Message
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

object MainSpec {
  val queueName = "testQueue2"
  val testSqsEndpoint = "http://localhost:4576"
  val sqsVisibilityTimeout = 1.second
  val sourceSettings = SqsSourceSettings()
    .withCloseOnEmptyReceive(true)    // When there are no more messages, close down the pipeline
    .withWaitTime(10.milliseconds)    // Wait 10ms for messages before closing
    .withVisibilityTimeout(sqsVisibilityTimeout)  // sqs message visibility timeout
}

import com.hombredequeso.sqstoes.test.SqsServiceSpec._

case class Entity(id: Int, description: String)

class MainSpec
  extends SqsTestBase(
    queueName,
    testSqsEndpoint) {


  "Pipeline" should "take message from sqs and put in es then delete message" in {

    val (sqsSource, sqsSink): (Source[Message, UniqueKillSwitch], Sink[MessageAction, Future[Done]]) =
      new SqsService(queueUrl).create(
        queueUrl,
        maxMessagesInFlight = 10,   // maximum number of messages being processed by AmazonSQSAsync at the same time
        sourceSettings= sourceSettings)

    val esWriterSettings = ElasticsearchWriteSettings()
      .withBufferSize(20)   // size of bulk POSTS to es when back-pressure applies
      .withVersionType("internal")
      .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))

    implicit val client: RestClient = RestClient.builder(new HttpHost("localhost", 9200)).build()
    implicit val format: JsonFormat[Entity] = jsonFormat2(Entity)

    val esFlow: Flow[WriteMessage[Entity, Message], WriteResult[Entity, Message], NotUsed] =
      ElasticsearchFlow.createWithPassThrough[Entity, Message]("entity3", "_doc", esWriterSettings)

    val pipeline: RunnableGraph[(UniqueKillSwitch, Future[Done])] = sqsSource
      // Create the elasticsearch upsertMessage, with the Sqs Message as the pass through
      .map(m => {
        val entity = m.body().parseJson.convertTo[Entity]
        WriteMessage.createUpsertMessage(entity.id.toString, entity).withPassThrough(m)
      })
      // Write to es
      .via(esFlow)
      // .wireTap(x => println(s"Result: ${x._2.success}; message = ${x._1}"))
      // Only continue to delete the sqs message if it was successful
      .filter(_.success)
      .map(x => MessageAction.delete(x.message.passThrough))
      .toMat(sqsSink)(Keep.both)

    val entityList  =
      List.range(0, 20)
      .map(i => Entity(i, s"Entity $i"))
      .map(e => e.toJson.compactPrint)

    for {
      _ <- Future.traverse(entityList)(SqsQueue.sendMessage(queueUrl, _))
      _ <- pipeline.run()._2
      queueIsEmptyAssertion <- SqsQueue.assertQueueEmpty(sqsVisibilityTimeout, queueUrl)
    } yield queueIsEmptyAssertion
  }
}
