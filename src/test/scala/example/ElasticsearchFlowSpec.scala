package example

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchFlow
import akka.stream.{ActorMaterializer, FlowShape, Graph}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source, ZipWith}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import spray.json._
import DefaultJsonProtocol._
import akka.stream.alpakka.elasticsearch._

class ElasticsearchFlowSpec
  extends FlatSpec
    with Matchers {

  case class Entity(id: Int, description: String)

  implicit val client: RestClient = RestClient.builder(new HttpHost("localhost", 9200)).build()
  implicit val format: JsonFormat[Entity] = jsonFormat2(Entity)

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()

  "Simple Stream" should "be able to be run in a test" in {

    val esWriterSettings = ElasticsearchWriteSettings()
      .withBufferSize(2)
      .withVersionType("internal")
      .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))

    val entities = (1 to 10).map(i => Entity(i, s"entity #${i}"))
    val source = Source(entities)
    val esFlow: Flow[WriteMessage[Entity, NotUsed], WriteResult[Entity, NotUsed], NotUsed] =
      ElasticsearchFlow.create[Entity]("entity", "_doc", esWriterSettings)
    val sink = Sink.ignore

    // val pipeline: RunnableGraph[Future[Done]] =
      // val r: Future[Seq[Unit]] = source
    val r = source
        .map(e => WriteMessage.createUpsertMessage(e.id.toString, e))
        .via(esFlow)
      .map(wr => println(s"Success: ${wr.success}; message: ${wr.message}"))
      .runWith(sink)
        // .runWith(Sink.seq)
        // .toMat(sink)(Keep.right)
        // pipeline.run()

    Await.result(r, 3.seconds)
  }

  case class Message[T](e: T)

  "Elasticsearch Stream" should "use passthrough to maintain original message" in {
    val esWriterSettings = ElasticsearchWriteSettings()
      .withBufferSize(5)
      .withVersionType("internal")
      .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))

    val entities: Seq[Message[Entity]] =
      (1 to 50)
        .map(i => Entity(i, s"entity #${i}"))
        .map(Message(_))

    val source = Source(entities)
    val esFlow =
      ElasticsearchFlow.createWithPassThrough[Entity, Message[Entity]]("entity2", "_doc", esWriterSettings)

    val r: Future[Seq[Message[Entity]]] = source
      .map(m => WriteMessage.createUpsertMessage(m.e.id.toString, m.e).withPassThrough(m))
      .via(esFlow)
      .map(wr => (wr.message.passThrough, wr.success, wr))
      .map({case((msg, success, wr)) => {
        println(s"Result: ${success}; message = ${msg}")
        msg
      }})

      .runWith(Sink.seq)

    val result: Seq[Message[Entity]] =
      Await.result(r, 3.seconds)
    result.sortBy(x => x.e.id) should ===(entities)
  }
}
