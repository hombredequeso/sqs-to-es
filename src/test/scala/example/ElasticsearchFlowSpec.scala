package example

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchFlow
import akka.stream.{ActorMaterializer, FlowShape, Graph}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source, ZipWith}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import spray.json._
import DefaultJsonProtocol._
import akka.stream.alpakka.elasticsearch._

import scala.concurrent.ExecutionContext.Implicits.global

class ElasticsearchFlowSpec extends FlatSpec with Matchers {

  case class Entity(id: Int, description: String)
  case class Message[T](e: T)

  implicit val client: RestClient = RestClient.builder(new HttpHost("localhost", 9200)).build()
  implicit val format: JsonFormat[Entity] = jsonFormat2(Entity)

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()

  val esWriterSettings = ElasticsearchWriteSettings()
    .withBufferSize(2)
    .withVersionType("internal")
    .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))


  "Simple Stream" should "be able to be run in a test" in {

    val entities =
      (1 to 10).map(i => Entity(i, s"entity #${i}"))
    val esFlow: Flow[WriteMessage[Entity, NotUsed], WriteResult[Entity, NotUsed], NotUsed] =
      ElasticsearchFlow.create[Entity]("entity", "_doc", esWriterSettings)

    for {
      writeResults <- Source(entities)
        .map(e => WriteMessage.createUpsertMessage(e.id.toString, e))
        .via(esFlow)
        .wireTap(x => println(s"Result: ${x.success}"))
        .runWith(Sink.seq)
      assertAllMessagesProcessed = writeResults.length should ===(entities.length)
    } yield assertAllMessagesProcessed
  }


  "Elasticsearch Stream" should "use passthrough to maintain original message" in {

    val sourceMessages: Seq[Message[Entity]] =
      (1 to 50)
        .map(i => Entity(i, s"entity #${i}"))
        .map(Message(_))

    val esFlow =
      ElasticsearchFlow.createWithPassThrough[Entity, Message[Entity]]("entity2", "_doc", esWriterSettings)

    for {
      processedMessages  <-
      Source(sourceMessages)
        .map(m => WriteMessage.createUpsertMessage(m.e.id.toString, m.e).withPassThrough(m))
        .via(esFlow)
        .map(wr => (wr.message.passThrough, wr))
        .wireTap(x => println(s"Result: ${x._2.success}; message = ${x._1}"))
        .filter(_._2.success)
        .runWith(Sink.seq)
      assertion = processedMessages .map(_._1).sortBy(x => x.e.id) should ===(sourceMessages)
    } yield assertion
  }
}
