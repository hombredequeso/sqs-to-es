package com.hombredequeso.sqstoes

import java.net.URI

import akka.actor.ActorSystem
import akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchFlow
import akka.stream.alpakka.elasticsearch.{ElasticsearchWriteSettings, RetryAtFixedRate, WriteMessage, WriteResult}
import akka.stream.alpakka.sqs.{MessageAction, SqsSourceSettings}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source, ZipWith}
import akka.stream.{ActorMaterializer, FlowShape, Graph, UniqueKillSwitch}
import akka.{Done, NotUsed}
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import software.amazon.awssdk.services.sqs.model.Message
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.StdIn
import scala.language.postfixOps

//object PassThroughFlow {
//  def apply[A, T](processingFlow: Flow[A, T, NotUsed]): Graph[FlowShape[A, (T, A)], NotUsed] =
//    apply[A, T, (T, A)](processingFlow, Keep.both)
//
//  def apply[A, T, O](processingFlow: Flow[A, T, NotUsed], output: (T, A) => O): Graph[FlowShape[A, O], NotUsed] =
//    Flow.fromGraph(GraphDSL.create() { implicit builder =>
//      {
//        import GraphDSL.Implicits._
//
//        val broadcast = builder.add(Broadcast[A](2))
//        val zip = builder.add(ZipWith[T, A, O]((left, right) => output(left, right)))
//
//        // format: off
//        broadcast.out(0) ~> processingFlow ~> zip.in0
//        broadcast.out(1)         ~>           zip.in1
//        // format: on
//
//        FlowShape(broadcast.in, zip.out)
//      }
//    })
//}

case class Entity(id: Int, description: String)
// {'id':123,'description':'entity123'}

object Main extends App {

  val queueUrl = "http://localhost:4576/queue/mainqueue"
  val esUrl = "http://localhost:9200"

  // Make queue. See:
  // https://lobster1234.github.io/2017/04/05/working-with-localstack-command-line/

  implicit val system = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  val sourceSettings = SqsSourceSettings()
    .withVisibilityTimeout(5.second)  // sqs message visibility timeout
//    .withCloseOnEmptyReceive(true)    // When there are no more messages, close down the pipeline
//    .withWaitTime(10.milliseconds)    // Wait 10ms for messages before closing

  val sqsService = new SqsService(queueUrl)

  val (sqsSource, sqsSink): (Source[Message, UniqueKillSwitch], Sink[MessageAction, Future[Done]]) =
    sqsService.create(
      queueUrl,
      maxMessagesInFlight = 10,   // maximum number of messages being processed by AmazonSQSAsync at the same time
      sourceSettings= sourceSettings)

  val esWriterSettings = ElasticsearchWriteSettings()
    .withBufferSize(20)   // size of bulk POSTS to es when back-pressure applies
    .withVersionType("internal")
    .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))

  val uri = new URI(esUrl)
  val esHttpHost = new HttpHost(uri.getHost, uri.getPort, uri.getScheme)
  implicit val client: RestClient = RestClient.builder(esHttpHost).build()
  implicit val format: JsonFormat[Entity] = jsonFormat2(Entity)

  val esFlow: Flow[WriteMessage[Entity, Message], WriteResult[Entity, Message], NotUsed] =
    ElasticsearchFlow.createWithPassThrough[Entity, Message]("entityx", "_doc", esWriterSettings)

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

  val (killSwitch, futureDone): (UniqueKillSwitch, Future[Done]) = pipeline.run()

  println(s"Running service. Press enter to stop.")
  StdIn.readLine()

  killSwitch.shutdown()
  Await.ready(futureDone, 10 seconds)
  sqsService.stop()
  system.terminate()
}
