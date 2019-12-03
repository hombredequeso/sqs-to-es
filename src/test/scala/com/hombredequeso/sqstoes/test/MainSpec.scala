package com.hombredequeso.sqstoes.test

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

class MainSpec
  extends TestKit(ActorSystem("TestSystem"))
    with AsyncFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

}
