package org.constellation.util

import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.constellation.{ConstellationNode, HostPort}
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.{IncrementMetric, UpdateMetric}

import scala.concurrent.ExecutionContext
import scala.util.Try

object TestNode {

  private var nodes = Seq[ConstellationNode]()

  def apply(seedHosts: Seq[HostPort] = Seq(),
            keyPair: KeyPair = KeyUtils.makeKeyPair(),
            randomizePorts: Boolean = true,
            portOffset: Int = 0,
            numPartitions: Int = 2
           )(
    implicit system: ActorSystem,
    materializer: ActorMaterializer,
    executionContext: ExecutionContext
  ): ConstellationNode = {

    val randomPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 9000 + portOffset
    val randomPeerPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 9001 + portOffset
    val randomPeerTCPPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 9002 + portOffset
    val randomUDPPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 16180 + portOffset

    val node = new ConstellationNode(
      keyPair,
      seedHosts,
      "0.0.0.0",
      randomPort,
      udpPort = randomUDPPort,
      autoSetExternalAddress = true,
      peerHttpPort = randomPeerPort,
      peerTCPPort = randomPeerTCPPort,
      attemptDownload = seedHosts.nonEmpty,
      allowLocalhostPeers = true
    )


    nodes = nodes :+ node

    node.dao.partition = (portOffset / 2) % numPartitions

    node.dao.metricsManager ! UpdateMetric("partition", node.dao.partition.toString)

    node.logger.info(s"Started testnode on partition ${node.dao.partition}")

    node.dao.processingConfig = node.dao.processingConfig.copy(
      numFacilitatorPeers = 1,
      minCheckpointFormationThreshold = 3,
      randomTXPerRoundPerPeer = 0,
      metricCheckInterval = 10,
      maxWidth = 3,
      maxMemPoolSize = 6,
      minPeerTimeAddedSeconds = 1,
      snapshotInterval = 5,
      snapshotHeightInterval = 2,
      snapshotHeightDelayInterval = 1,
      numPartitions = numPartitions
    )

    node
  }

  def clearNodes(): Unit = {
    Try {
      nodes.foreach { node => node.shutdown() }
      nodes = Seq()
    }
  }

}
