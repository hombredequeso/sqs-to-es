package example

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

class MainSpec
  extends TestKit(ActorSystem("TestSystem"))
    with AsyncFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

}
