
import java.net.URI
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream._
import akka.stream.alpakka.sqs.{MessageAction, SqsSourceSettings}
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}
import akka.testkit.{ImplicitSender, TestActors, TestKit, TestProbe}
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import org.scalatest._
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.concurrent.java8.FuturesConvertersImpl.{CF, P}
import scala.language.postfixOps
import scala.util.Try


class SqsServiceSpec
  extends TestKit(ActorSystem("TestSystem"))
    with ImplicitSender
    with AsyncFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  var queueUrl = ""   // Set as part of the test.
  val queueName = "testQueue"
  val testSqsEndpoint = "http://localhost:4576"
  val sqsVisibilityTimeout = 1.second

  val messageBody = "Example Body"
  val messages = List(messageBody, "msg2")

  implicit val awsSqsClient: SqsAsyncClient = SqsAsyncClient
    .builder()
    .endpointOverride(URI.create(testSqsEndpoint))
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
    .region(Region.EU_CENTRAL_1)
    .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
    .build()

  override def afterAll(): Unit = {
    awsSqsClient.close()
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  override def beforeEach(): Unit = {

    val createQueueRequest: CreateQueueRequest = CreateQueueRequest.builder().queueName(queueName).build()
    val queue: CompletableFuture[CreateQueueResponse] = awsSqsClient.createQueue(createQueueRequest)
    queueUrl = queue.get().queueUrl()
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    val deleteQueueRequest: DeleteQueueRequest =
      DeleteQueueRequest.builder.queueUrl(queueUrl).build()
    awsSqsClient.deleteQueue(deleteQueueRequest).get()
    super.afterEach()
  }

  "SqsService" should "receive a message" in {

    val ec: ExecutionContext = this.executionContext
    implicit val mat: ActorMaterializer = ActorMaterializer()

    val sourceSettings = SqsSourceSettings()
      .withCloseOnEmptyReceive(true)    // When there are no more messages, close down the pipeline
      .withWaitTime(10.milliseconds)    // Wait 10ms for messages before closing
      .withVisibilityTimeout(sqsVisibilityTimeout)  // sqs message visibility timeout

    val (sqsSource, sqsSink): (Source[Message, UniqueKillSwitch], Sink[MessageAction, Future[Done]]) =
      new SqsService(queueUrl).create(
        queueUrl,
        maxMessagesInFlight = 20,
        sourceSettings= sourceSettings)

    val probe = TestProbe()

    val pipeline: RunnableGraph[(UniqueKillSwitch, Future[Done])] = sqsSource
      .wireTap(probe.ref ! _.body)
      .map(MessageAction.delete(_))
      .toMat(sqsSink)(Keep.both)

    val endAssertion = for {
      _ <- Future.traverse(messages)(new SqsQueue().sendMessage(queueUrl, _))
      _ <- pipeline.run()._2
      _ <- akka.pattern.after(500.milliseconds, using = system.scheduler)(Future(Done.done()))
      _ = probe.receiveN(messages.length) should ===(messages)
      _ <- akka.pattern.after(2*sqsVisibilityTimeout, using = system.scheduler)(Future(Done.done()))
      messagesLeft <- new SqsQueue().hasAnyMessages(queueUrl)
      assertNoMessagesLeft = messagesLeft should === (false)
    } yield assertNoMessagesLeft
    endAssertion
  }

}

class SqsQueue(implicit awsSqsClient: SqsAsyncClient, ec: ExecutionContext) {
  def hasAnyMessages(queueUrl: String): Future[Boolean] =
    getQueueMessage(queueUrl).map(resp => resp.messages().size() > 0)

  def getQueueMessage(queueUrl: String): Future[ReceiveMessageResponse] = {
    val receiveMessageRequest: ReceiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl).maxNumberOfMessages(10).build()
    val x = awsSqsClient.receiveMessage(receiveMessageRequest)
    toScala(x)
  }

  def sendMessage(queueUrl: String, msgBody: String) = {
    val sendMessageRequest: SendMessageRequest =
      SendMessageRequest.builder().queueUrl(queueUrl).messageBody(msgBody).build()
    toScala(awsSqsClient.sendMessage(sendMessageRequest))
  }

  // Convert java CompletableFuture to a scala Future
  def toScala[T](cs: CompletionStage[T]): Future[T] = {
    cs match {
      case cf: CF[T] => cf.wrapped
      case _ =>
        val p = new P[T](cs)
        cs whenComplete p
        p.future
    }
  }

}

