package org.constellation.consensus

import java.security.KeyPair

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import org.constellation.consensus.NodeRemoteSender.BroadcastRoundStartNotification
import org.constellation.consensus.RandomData.keyPairs
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema.{Id, SignedObservationEdge}
import org.constellation.primitives.{PeerData, ThreadSafeTipService}
import org.constellation.util.{APIClient, HashSignature, Metrics, SignatureBatch}
import org.constellation.{DAO, Fixtures, PeerMetadata}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global

class NodeConsensusTest extends TestKit(ActorSystem("test"))
  with FunSpecLike with Matchers with BeforeAndAfter with ImplicitSender with MockFactory {

  private implicit val config: Config = ConfigFactory.empty()
  private implicit var fakeDao: DAO = _
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val keyPair: KeyPair = keyPairs.head

  private implicit var fakeThreadSafeTipService: ThreadSafeTipService = _

  private var nodeActor: TestActorRef[Node] = _
  private var remoteSenderProbe: TestProbe = _
  val sampleRoundId = RoundId("round-id-1")

  val sampleFacilitators: (Seq[SignedObservationEdge], Map[Id, PeerData]) = {
    (Seq(SignedObservationEdge(SignatureBatch("1", Seq(HashSignature("1", Id("1")))))),
      Map(Id("1") -> PeerData(PeerMetadata("localhost", 0, 0, Id("1")), APIClient.apply(port = 9999))))
  }
  
  before {
    remoteSenderProbe = TestProbe("remote-sender-probe")
    fakeDao = stub[DAO]
    (fakeDao.id _).when().returns(Fixtures.id)
    (fakeDao.pullTips _).when(false).returns(Some(sampleFacilitators))

    fakeDao.keyPair = KeyUtils.makeKeyPair()
    fakeDao.metrics = new Metrics()
    val peerProbe = TestProbe.apply("peerManager")
    fakeDao.peerManager = peerProbe.ref

    nodeActor = TestActorRef(new Node(remoteSenderProbe.ref))
  }

  describe("Node") {
    it("should broadcast block building round to facilitators") {
      val peerData = fakeDao.pullTips()
        .map(_._2).map(t => t.values).get.toSeq

      nodeActor ! NotifyFacilitators(sampleRoundId)
      remoteSenderProbe.expectMsg(BroadcastRoundStartNotification(sampleRoundId, peerData))
    }
  }


}
