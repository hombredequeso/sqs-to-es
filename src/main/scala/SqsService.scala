import java.util.concurrent.CompletableFuture

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.sqs.scaladsl.{SqsAckSink, SqsSource}
import akka.stream.alpakka.sqs.{MessageAction, SqsAckSettings, SqsSourceSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.typesafe.scalalogging.StrictLogging
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{GetQueueAttributesRequest, ListQueuesRequest, ListQueuesResponse, Message, QueueDoesNotExistException}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import java.net.URI

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

  def findAvailableQueues(queueNamePrefix: String): Seq[String] = {
    val request: ListQueuesRequest =
      ListQueuesRequest.builder().queueNamePrefix(queueNamePrefix).build()
    sqsClient.listQueues(request).get().queueUrls().asScala.toVector
  }

  def assertQueueExists(queueUrl: String): Unit =
    try {
      val attr: GetQueueAttributesRequest =
        GetQueueAttributesRequest.builder().queueUrl(queueUrl).build()
      sqsClient.getQueueAttributes(attr)
      logger.info(s"Queue at $queueUrl found.")
    } catch {
      case queueDoesNotExistException: QueueDoesNotExistException =>
        logger.error(s"The queue with url $queueUrl does not exist.")
        throw queueDoesNotExistException
    }



  import scala.concurrent.duration._

  def create
    (queueUrl: String, maxMessagesInFlight: Int)(
      implicit system: ActorSystem, mat: ActorMaterializer)
  : (Source[Message, UniqueKillSwitch], Sink[MessageAction, Future[Done]]) = {

    assertQueueExists(queueUrl)

    val sourceSettings = SqsSourceSettings()
      .withCloseOnEmptyReceive(true)
      .withWaitTime(10.milliseconds)
      .withVisibilityTimeout(1.second)

    val source: Source[Message, UniqueKillSwitch] =
      SqsSource(queueUrl, sourceSettings).viaMat(KillSwitches.single)(Keep.right)
    val settings: SqsAckSettings = SqsAckSettings(maxMessagesInFlight)

    val sink: Sink[MessageAction, Future[Done]] = SqsAckSink(queueUrl, settings)


//    val flow: Flow[Message, Message, NotUsed] = {
//        Flow.fromFunction(handleMessage(messageHandler))
//          .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
//    }
//    val result: (KillSwitch,Future[Done]) = source
//      .via(flow)
//      .map(MessageAction.delete(_))
//      .toMat(sink)(Keep.both)
//      .run()

    (source, sink)
  }

  import software.amazon.awssdk.services.sqs.model.Message

  private def handleMessage(messageHandler: MyMessage => Unit)
  : Message => Message = {
    message: Message =>
      messageHandler(MyMessage(message.body()))
      message
  }
}
