package org.constellation.metrics

import akka.actor.{Actor, ActorLogging}
import com.typesafe.scalalogging.Logger

object MetricsActor {
  trait MRecord
  case class MDBDelete(count: Int) extends MRecord
  case class MDBGet(count: Int) extends MRecord
  case class MDBPut(count: Int) extends MRecord
}


class MetricsActor extends Actor with ActorLogging {
  import MetricsActor._
  val logger = Logger(s"PeerToPeer")
  override def receive: Receive = {

    case a@MDBPut(_) => log.warning(s"MetricsActor -- {}", a)
    case a@MDBGet(_) => logger.warn(s"MetricsActor -- $a")
    case a@MDBDelete(_) => logger.warn(s"MetricsActor -- $a")


  }
}
