package org.constellation.primitives

import java.util.Collections

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.scalalogging.Logger
import io.prometheus.client.CollectorRegistry
import org.constellation.DAO
import org.constellation.crypto.KeyUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class MetricsManagerTest()
    extends TestKit(ActorSystem("ConstellationTest"))
    with Matchers
    with FlatSpecLike
    with BeforeAndAfterAll {

  val logger = Logger("ConstellationTest")
  logger.info("MetricsManagerTest init")

  override def afterAll: Unit = {
    logger.info("Shutting down the Actor under test")
    shutdown(system)
  }

  logger.info("Initializing the DAO actor")
  implicit val dao: DAO = new DAO()
  dao.updateKeyPair(KeyUtils.makeKeyPair())
  dao.idDir.createDirectoryIfNotExists(createParents = true)
  dao.preventLocalhostAsPeer = false
  dao.externalHostString = ""
  dao.externlPeerHTTPPort = 0
  logger.info("DAO actor initialized")

  logger.info("MetricsManager actor initialized")

  "MetricsManager" should "report micrometer metrics" in {
    val familySamples = Collections.list(CollectorRegistry.defaultRegistry.metricFamilySamples())
    familySamples.size() should be > 0
  }
}
