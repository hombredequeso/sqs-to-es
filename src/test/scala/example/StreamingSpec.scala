package example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, FlowShape, Graph}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Sink, Source, ZipWith}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

// Reference:
// https://doc.akka.io/docs/alpakka/current/patterns.html#passthrough

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

class StreamingSpec
    extends FlatSpec
    with Matchers {


  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()

  "Simple Stream" should "be able to be run in a test" in {
    val sinkUnderTest: Sink[Int, Future[Int]] =
      Flow[Int].map(_ * 2).toMat(Sink.fold(0)(_ + _))(Keep.right)

    val future: Future[Int] = Source(1 to 4).runWith(sinkUnderTest)
    val result: Int = Await.result(future, 3.seconds)
    assert(result == 20)
  }

  import akka.stream.ActorAttributes.supervisionStrategy
  import akka.stream.Supervision.resumingDecider

  "Exceptions" should "be able to be handled gracefully" in {
    val ints = List("1", "2", "not-a-number", "3")
    val source: Source[String, NotUsed] =
      Source(ints)
    val sink: Sink[Int, Future[Int]] =
      Flow[Int].toMat(Sink.fold(0)(_ + _))(Keep.right)


    // On error handling:
    // https://doc.akka.io/docs/akka/current/stream/stream-error.html
    val future = source
      .map(_.toInt)
      .withAttributes(supervisionStrategy(resumingDecider))
      .runWith(sink)

    val result: Int = Await.result(future, 3.seconds)
    assert(result == 6)
  }

  "Pass through" should "unzip and zip correctly" in {
    val source = Source(List(1, 2, 3))

    val passThroughMe =
      Flow[Int]
        .map(_.toString)

    val ret: Future[Seq[(String, Int)]] = source
      .via(PassThroughFlow(passThroughMe))
      .runWith(Sink.seq)

    val result = Await.result(ret, 3.seconds)
    result should be(Vector(("1", 1), ("2", 2), ("3", 3)))
  }

  def myFunc(i: Int) : String = {
    if (i == 2) {
      throw new Exception("epic fail")
    }else
      i.toString
  }


  import scala.util.Try

  "Pass through" should "unzip and zip correctly even where there are errors" in {
    val source = Source(List(1, 2, 3))

    val myFunc2: Int => Either[Throwable, String] = (i: Int) => Try(myFunc(i)).toEither

    val passThroughMe =
      Flow[Int]
        .map(myFunc2)

    val mainFlow: Graph[FlowShape[Int, (Either[Throwable, String], Int)], NotUsed] = PassThroughFlow(passThroughMe)

    val ret: Future[Seq[(String, Int)]] = source
      .via(PassThroughFlow(passThroughMe))
      .collect{case (Right(s), i) => (s, i)}
      .runWith(Sink.seq)

    val result = Await.result(ret, 3.seconds)
    result should ===(Vector(("1", 1), ("3", 3)))
  }

}
