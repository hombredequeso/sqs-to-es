package com.hombredequeso.sqstoes

import java.nio.file.{Path, Paths}

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, FlowShape, Graph, UniqueKillSwitch}
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Keep, Sink, Source, ZipWith}
import akka.util.ByteString
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import spray.json._
import DefaultJsonProtocol._
import akka.stream.alpakka.elasticsearch.{ElasticsearchWriteSettings, RetryAtFixedRate, WriteMessage, WriteResult}
import akka.stream.alpakka.elasticsearch.scaladsl.{ElasticsearchFlow, ElasticsearchSink}
import akka.stream.alpakka.sqs.MessageAction
import software.amazon.awssdk.services.sqs.model.Message

import scala.concurrent.duration._
import scala.concurrent.Future

case class Person(id: String, firstName: String, lastName: String)

object PassThroughFlow {
  def apply[A, T](processingFlow: Flow[A, T, NotUsed]): Graph[FlowShape[A, (T, A)], NotUsed] =
    apply[A, T, (T, A)](processingFlow, Keep.both)

  def apply[A, T, O](processingFlow: Flow[A, T, NotUsed], output: (T, A) => O): Graph[FlowShape[A, O], NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      {
        import GraphDSL.Implicits._

        val broadcast = builder.add(Broadcast[A](2))
        val zip = builder.add(ZipWith[T, A, O]((left, right) => output(left, right)))

        // format: off
        broadcast.out(0) ~> processingFlow ~> zip.in0
        broadcast.out(1)         ~>           zip.in1
        // format: on

        FlowShape(broadcast.in, zip.out)
      }
    })
}

object Main extends App {

//  def transform(m: Message): Person = {
//    val content = m.body()
//    content.parseJson.convertTo[Person]
//  }
//
//
//  implicit val system = ActorSystem("QuickStart")
//  implicit val materializer = ActorMaterializer()
//
//
//  implicit val client: RestClient = RestClient.builder(new HttpHost("localhost", 9200)).build()
//
//  implicit val format: JsonFormat[Person] = jsonFormat3(Person)
//
//  val currentDirectory = new java.io.File(".").getCanonicalPath
//  printf(currentDirectory)
//
//  var id = 0;
//  val source = Source.single(ByteString("eins,zwei,drei\nharry;highpants"))
//
//  var path = Paths.get("data/names.csv")
//
//  val fileSource = FileIO.fromPath(path)
//
//  val processCsv: Flow[ByteString, List[String], NotUsed] = Flow[ByteString]
//    .via(CsvParsing.lineScanner())
//    .map(_.map(_.utf8String))
//
//
//  val fieldsToPerson: Flow[List[String], Person, NotUsed] =
//    Flow[List[String]].map(ss => {
//      id = id + 1;
//      Person(id.toString, ss(0), ss(1))
//    })
//
//  val sinkSettings = ElasticsearchWriteSettings()
//    .withBufferSize(100)
//    .withVersionType("internal")
//    .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))
//
//  val esSink: Sink[WriteMessage[Person, NotUsed], Future[Done]] = ElasticsearchSink.create[Person](
//      "person",
//      typeName = "_doc",
//      sinkSettings
//    )
//
//  // Flow instead of sink.
//  val esFlow: Flow[WriteMessage[Person, NotUsed], WriteResult[Person, NotUsed], NotUsed] = ElasticsearchFlow.create(
//    "person",
//    typeName = "_doc",
//    sinkSettings
//    )
//
//
//  val transformFlow: Flow[Message, Person, NotUsed] = Flow[Message].map(transform _)
//
//  val msgToEs = esFlow.via(transformFlow)
//
//  val actuallyNeed: Flow[(Message, WriteMessage[Person, NotUsed]),(Message, WriteResult[Person, NotUsed]), NotUsed] = ???
////    Flow.
//
//
//  val actuallyNeed2: Flow[(Message, WriteMessage[Person, NotUsed]),(Message, WriteResult[Person, NotUsed]), NotUsed] = ???
//    Flow[(Message, WriteMessage[Person, NotUsed])].map({case(msg, writeThingy) => esFlow(writeThingy)}).putItAllBackTogetherNowPeople
//
//  val sqsEndpoint: String = ???
//
//  val queueUrl: String = ???
//
//  val (sqsSource: Source[Message, UniqueKillSwitch],
//      sqsSink: Sink[MessageAction, Future[Done]]) =
//    new SqsService(sqsEndpoint).create(queueUrl, 10)
//
//  val s: Sink[Nothing, Future[Seq[Nothing]]] = Sink.seq;
//
//  implicit val ec = system.dispatcher
//
//  val myStream2 = sqsSource .map(m => (m, transform(_)))
//    .map ( x => {
//      val (msg: Message, person: Person) = x
//      val r: WriteMessage[Person, NotUsed] = WriteMessage.createIndexMessage(person.id, person)
//      (msg, r)
//      r
//    })
//    .via(esFlow)
//
//    .map(MessageAction.delete(_))
//    .runWith(sqsSink)
//
//
//  val myStream3 = sqsSource
//    .map(m => (m, transform(_)))
//
//    .map ( x => {
//      val (msg: Message, person: Person) = x
//      val r: WriteMessage[Person, NotUsed] = WriteMessage.createIndexMessage(person.id, person)
//      val result: (Message, WriteMessage[Person, NotUsed]) = (msg, r)
//      result
//    })
//    .via(actuallyNeed)
//    // .via(esFlow)
//    .map({case (msg, _) => MessageAction.delete(msg)})
//    .runWith(sqsSink)
//
//  // SQS source
//  // transform
//  // esFlow
//  // SQS delete action
//  // SQS delete.
//
//  val myStream = fileSource
//    .via(processCsv)
//    .via(fieldsToPerson)
//    .map { person: Person => {
//      WriteMessage.createIndexMessage(person.id, person);
//    }
//    }
//    .runWith(esSink)
//  // val myStream = fileSource.via(processCsv).runForeach(ss => println(ss.mkString("::")))
//  // val myStream = source.via(processCsv).runForeach(ss => println(ss.mkString("::")))
//
//  // implicit val ec = system.dispatcher
//  myStream.onComplete(_ => system.terminate())

  println("Finished!")
}
