
import java.net.URI
import java.util.concurrent.CompletableFuture

import akka.Done
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.sqs.MessageAction
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.testkit.TestKit
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import org.scalatest._
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import scala.concurrent.Future
import scala.language.postfixOps


class MainSpec
  extends TestKit(ActorSystem("TestSystem"))
    with AsyncFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  var queueUrl = ""
  implicit val awsSqsClient: SqsAsyncClient = SqsAsyncClient
    .builder()
    .endpointOverride(URI.create("http://localhost:4576"))
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
    .region(Region.EU_CENTRAL_1)
    .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
    .build()

  override def afterAll(): Unit = {
    awsSqsClient.close()
    TestKit.shutdownActorSystem(system)
    // shutdown(system)
    // super.afterAll()
  }


  val queueName = "testQueue"

  override def beforeEach(): Unit = {

    val createQueueRequest: CreateQueueRequest = CreateQueueRequest.builder().queueName(queueName).build()
    val queue: CompletableFuture[CreateQueueResponse] = awsSqsClient.createQueue(createQueueRequest)
    queueUrl = queue.get().queueUrl()
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    val deleteQueueRequest: DeleteQueueRequest =
      DeleteQueueRequest.builder.queueUrl(queueUrl).build()
    val result: CompletableFuture[DeleteQueueResponse] = awsSqsClient.deleteQueue(deleteQueueRequest)
    val resultExecuted: DeleteQueueResponse = result.get()
    super.afterEach()
  }

  case class TestMessage(context: String)

  val messageBody = "Example Body"

  "SqsService" should "receive a message" in {

    implicit val mat: ActorMaterializer = ActorMaterializer()
    // Arrange
    val x: (Source[Message, UniqueKillSwitch], Sink[MessageAction, Future[Done]]) =
      new SqsService(queueUrl).create(queueUrl, maxMessagesInFlight = 20)
    val (sqsSource, sqsSink) = x

    var processedMessageCount = 0

    // Send message to SQS (synchronous)
    val sendMessageRequest: SendMessageRequest =
      SendMessageRequest.builder().queueUrl(queueUrl).messageBody(messageBody).build()
    val result: CompletableFuture[SendMessageResponse] = awsSqsClient.sendMessage(sendMessageRequest)
    val resultExecuted = result.get()

    val pipeline: (UniqueKillSwitch, Future[Done]) = sqsSource
      .map(m => {
        processedMessageCount = processedMessageCount + 1
        m
      })
      .map(m => {
        val r= MessageAction.delete(m)
        r
      })
      .toMat(sqsSink)(Keep.both)
      .run()

    val resultAssertion: Future[Assertion] = pipeline._2.map[Assertion](_ => {
      processedMessageCount should ===(1)
    })

    resultAssertion
  }
}

