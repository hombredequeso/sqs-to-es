package com.hombredequeso.sqstoes

import java.net.URI

import akka.Done
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.sqs.scaladsl.{SqsAckSink, SqsSource}
import akka.stream.alpakka.sqs.{MessageAction, SqsAckSettings, SqsSourceSettings}
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{GetQueueAttributesRequest, Message, QueueDoesNotExistException}

import scala.concurrent.Future

class SqsService(sqsEndpoint: String)(implicit system: ActorSystem) extends StrictLogging {

  case class MyMessage(content: String)

  implicit val sqsClient: SqsAsyncClient = SqsAsyncClient
  .builder()
  .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
  .endpointOverride(URI.create(sqsEndpoint))
  .region(Region.EU_CENTRAL_1)
  .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
  .build()

  def stop(): Unit = sqsClient.close()

  def assertQueueExists(queueUrl: String): Unit =
    try {
      sqsClient.getQueueAttributes(
        GetQueueAttributesRequest.builder().queueUrl(queueUrl).build())
        .get()
      logger.info(s"Queue at $queueUrl found.")
    } catch {
      case queueDoesNotExistException: QueueDoesNotExistException =>
        logger.error(s"The queue with url $queueUrl does not exist.")
        throw queueDoesNotExistException
    }

  def create
    (queueUrl: String,
     maxMessagesInFlight: Int = 10,
     sourceSettings: SqsSourceSettings = SqsSourceSettings())(
      implicit system: ActorSystem, mat: ActorMaterializer)
  : (Source[Message, UniqueKillSwitch], Sink[MessageAction, Future[Done]]) = {

    assertQueueExists(queueUrl)

    val source: Source[Message, UniqueKillSwitch] =
      SqsSource(queueUrl, sourceSettings).viaMat(KillSwitches.single)(Keep.right)

    val settings: SqsAckSettings = SqsAckSettings(maxMessagesInFlight)
    val sink: Sink[MessageAction, Future[Done]] = SqsAckSink(queueUrl, settings)

    (source, sink)
  }
}
