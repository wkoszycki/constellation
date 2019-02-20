package org.constellation.consensus

import akka.actor.ActorRef
import org.constellation.primitives.PeerData

class HTTPNodeRemoteSender extends NodeRemoteSender {
  override def broadcastRoundStartNotification(roundId: RoundId, peerData: Seq[PeerData]): Unit = ???

  override def receiveRoundStartNotification(roundId: RoundId,
                                             peerData: Seq[PeerData],
                                             replyTo: ActorRef): Unit = ???
}
