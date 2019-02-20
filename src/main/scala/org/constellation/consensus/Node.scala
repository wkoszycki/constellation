package org.constellation.consensus
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{Backoff, BackoffSupervisor}
import org.constellation.DAO
import org.constellation.consensus.NodeRemoteSender.BroadcastRoundStartNotification
import org.constellation.primitives.PeerData

import scala.concurrent.duration._

class Node(remoteSenderSupervisor: ActorRef)(implicit dao: DAO) extends Actor with ActorLogging {
  val roundManagerProps: Props = RoundManager.props
  val roundManagerSupervisor: Props = BackoffSupervisor.props(
    Backoff.onFailure(
      roundManagerProps,
      childName = "round-manager",
      minBackoff = 3.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2
    )
  )
  val roundManager: ActorRef =
    context.actorOf(roundManagerSupervisor, name = "round-manager-supervisor")

  
  override def receive: Receive = {
    case StartBlockCreationRound =>
      roundManager ! StartBlockCreationRound

    case cmd: ReceivedProposal =>
      roundManager ! cmd

    case cmd: ReceivedMajorityUnionedBlock =>
      roundManager ! cmd

    case cmd: NotifyFacilitators =>
      val peerData = dao.pullTips()
        .map(_._2).map(t => t.values).fold(Seq.empty[PeerData])(p => p.toSeq)
      remoteSenderSupervisor ! BroadcastRoundStartNotification(cmd.roundId, peerData)

    case cmd: BroadcastProposal =>
      remoteSenderSupervisor ! cmd

    case cmd: BroadcastMajorityUnionedBlock =>
      remoteSenderSupervisor ! cmd

    case _                       => log.info("Received unknown message")
  }
}

object Node {
  def props(remoteSenderSupervisor: ActorRef)(implicit dao: DAO): Props = Props(new Node(remoteSenderSupervisor))
}
