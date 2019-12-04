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
import org.scalatest._
import software.amazon.awssdk.services.sqs.model._
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
case class PipelineMessage[T](e: T)

class MainSpec
  extends SqsTestBase(
    queueName,
    testSqsEndpoint) {


  "Pipeline" should "take message from sqs and put in es then delete message" in {

    val (sqsSource, sqsSink): (Source[Message, UniqueKillSwitch], Sink[MessageAction, Future[Done]]) =
      new SqsService(queueUrl).create(
        queueUrl,
        maxMessagesInFlight = 6,
        sourceSettings= sourceSettings)

    val esWriterSettings = ElasticsearchWriteSettings()
      .withBufferSize(1)
      .withVersionType("internal")
      .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))

    implicit val client: RestClient = RestClient.builder(new HttpHost("localhost", 9200)).build()
    implicit val format: JsonFormat[Entity] = jsonFormat2(Entity)

    val esFlow: Flow[WriteMessage[Entity, PipelineMessage[Message]], WriteResult[Entity, PipelineMessage[Message]], NotUsed] =
      ElasticsearchFlow.createWithPassThrough[Entity, PipelineMessage[Message]]("entity3", "_doc", esWriterSettings)

    val pipeline: RunnableGraph[(UniqueKillSwitch, Future[Done])] = sqsSource
      // wrap an Sqs Message in PipelineMessage wrapper
      .map(PipelineMessage(_))
      // Create the elasticsearch upsertMessage, with the PipelineMessage[Message] as the pass through
      .map(m => {
        val entity = m.e.body().parseJson.convertTo[Entity]
        WriteMessage.createUpsertMessage(entity.id.toString, entity).withPassThrough(m)
      })
      // Write to es
      .via(esFlow)
      // Get the passthrough PipelineMessage[Message] back.
      .map(wr => (wr.message.passThrough, wr))
      .wireTap(x => println(s"Result: ${x._2.success}; message = ${x._1}"))
      // Only continue if it was successful
      .filter(_._2.success)
      // Extract the original SQS Message
      .map(x => x._1.e)
      .map(MessageAction.delete(_))
      .toMat(sqsSink)(Keep.both)

    val entityList  =
      List.range(0, 19)
      .map(i => Entity(i, s"Entity $i"))
      .map(e => e.toJson.compactPrint)
    for {
      _ <- Future.traverse(entityList)(SqsQueue.sendMessage(queueUrl, _))
      _ <- pipeline.run()._2
      queueIsEmptyAssertion <- SqsQueue.assertQueueEmpty(sqsVisibilityTimeout, queueUrl)
    } yield queueIsEmptyAssertion
  }
}
