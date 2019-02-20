package org.constellation.consensus

import akka.actor.{Actor, ActorRef, Props}
import org.constellation.consensus.NodeRemoteSender.{BroadcastRoundStartNotification, RoundStartNotification}
import org.constellation.primitives.PeerData

object NodeRemoteSender {

  sealed trait NodeRemoteMessage

  case class BroadcastRoundStartNotification(roundId: RoundId, peerData: Seq[PeerData]) extends NodeRemoteMessage

  case class RoundStartNotification(roundId: RoundId, peerData: Seq[PeerData])
      extends NodeRemoteMessage

  def props(nodeRemoteSender: NodeRemoteSender): Props =
    Props(new NodeRemoteSenderStrategy(nodeRemoteSender))
}

trait NodeRemoteSender {
  def broadcastRoundStartNotification(roundId: RoundId, peerData: Seq[PeerData])

  def receiveRoundStartNotification(roundId: RoundId, peerData: Seq[PeerData], replyTo: ActorRef)
}

class NodeRemoteSenderStrategy(nodeRemoteSender: NodeRemoteSender) extends NodeRemoteSender with Actor {

  override def receive: Receive = {
    case BroadcastRoundStartNotification(roundId, peerData) =>
      broadcastRoundStartNotification(roundId, peerData)
    case RoundStartNotification(roundId, peerData) =>
      val replyTo = sender()
      receiveRoundStartNotification(roundId, peerData, replyTo)
  }

  override def broadcastRoundStartNotification(roundId: RoundId, peerData: Seq[PeerData]): Unit = {
    nodeRemoteSender.broadcastRoundStartNotification(roundId, peerData)
  }

  override def receiveRoundStartNotification(roundId: RoundId,
                                             peerData: Seq[PeerData],
                                             replyTo: ActorRef): Unit = {
    nodeRemoteSender.receiveRoundStartNotification(roundId, peerData, replyTo)
  }
}
