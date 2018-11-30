package org.constellation.consensus

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorSystem}
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.DAO
import org.constellation.consensus.EdgeProcessor.FinishedCheckpoint
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.util.{APIClient, HeartbeatSubscribe, ProductHash}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

object EdgeProcessor {

  case class HandleTransaction(tx: Transaction)
  case class HandleCheckpoint(checkpointBlock: CheckpointBlock)
  case class HandleSignatureRequest(checkpointBlock: CheckpointBlock)

  val logger = Logger(s"EdgeProcessor")
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)



  def calculateHeight(parentSOEBaseHashes: Seq[String])(implicit dao: DAO): Option[Height] = {

    val parents = parentSOEBaseHashes.map { h =>
      val maybeHeight = dao.checkpointService.get(h).flatMap{_.height}
      if (maybeHeight.nonEmpty) maybeHeight else {
        dao.soeService.get(h).flatMap(_.height)
      }
    }

    val maxHeight = if (parents.exists(_.isEmpty)) {
      None
    } else {

      val heights = parents.map{_.map(_.max)}

      val nonEmptyHeights = heights.flatten
      if (nonEmptyHeights.isEmpty) None else {
        Some(nonEmptyHeights.max + 1)
      }
    }

    val minHeight = if (parents.exists(_.isEmpty)) {
      None
    } else {

      val heights = parents.map{_.map(_.min)}

      val nonEmptyHeights = heights.flatten
      if (nonEmptyHeights.isEmpty) None else {
        Some(nonEmptyHeights.min + 1)
      }
    }

    val height = maxHeight.flatMap{ max =>
      minHeight.map{
        min =>
          Height(min, max)
      }
    }

    height

  }


  def acceptHeader(soeCache: SignedObservationEdgeCacheData)(implicit dao: DAO): Unit = {

    soeCache.signedObservationEdge match {
      case None =>
        dao.metricsManager ! IncrementMetric("acceptHeaderCalledWithEmptyCB")
      case Some(soe) =>

        val height = soeCache.calculateHeight()

        val fallbackHeight = if (height.isEmpty) soeCache.height else height

        if (fallbackHeight.isEmpty) {
          dao.metricsManager ! IncrementMetric("heightHeaderEmpty")
        } else {
          dao.metricsManager ! IncrementMetric("heightHeaderNonEmpty")
        }

        // TODO: Validate parent hashes match and partition as well
        dao.soeService.put(soe.baseHash, soeCache.copy(height = fallbackHeight))
        soeCache.diffMap.foreach{
          case (h, diff) =>
            dao.addressService.update(
              h,
              { a: AddressCacheData => a.copy(balance = a.balance + diff)},
              AddressCacheData(0L, 0L) // unused since this address should already exist here
            )
        }

    }

  }

  def acceptCheckpoint(checkpointCacheData: CheckpointCacheData)(implicit dao: DAO): Unit = {

    if (checkpointCacheData.checkpointBlock.isEmpty) {
      dao.metricsManager ! IncrementMetric("acceptCheckpointCalledWithEmptyCB")
    } else {

      val cb = checkpointCacheData.checkpointBlock.get

      val height = cb.calculateHeight()

      val fallbackHeight = if (height.isEmpty) checkpointCacheData.height else height

      if (fallbackHeight.isEmpty) {
        dao.metricsManager ! IncrementMetric("heightEmpty")
      } else {
        dao.metricsManager ! IncrementMetric("heightNonEmpty")
      }

      // Accept transactions
      cb.transactions.foreach { t =>
        dao.metricsManager ! IncrementMetric("transactionAccepted")
        t.store(
          TransactionCacheData(
            t,
            valid = true,
            inMemPool = false,
            inDAG = true,
            Map(cb.baseHash -> true),
            resolved = true,
            cbBaseHash = Some(cb.baseHash)
          ))
        t.ledgerApply()
      }
      dao.metricsManager ! IncrementMetric("checkpointAccepted")
      cb.store(
        checkpointCacheData.copy(height = fallbackHeight)
      )

    }
  }

  /**
    * Main transaction processing cell
    * This is triggered upon external receipt of a transaction. Assume that the transaction being processed
    * came from a peer, not an internal operation.
    * @param tx : Transaction with all data
    * @param dao : Data access object for referencing memPool and other actors
    * @param executionContext : Threadpool to execute transaction processing against. Should be separate
    *                         from other pools for processing different operations.
    */
  def handleTransaction(
                         tx: Transaction, skipValidation: Boolean = true
                       )(implicit executionContext: ExecutionContext, dao: DAO): Unit = {

    // TODO: Store TX in DB immediately rather than waiting
    // ^ Requires TX hash partitioning or conflict stuff later + gossiping tx's to wider reach

    dao.metricsManager ! IncrementMetric("transactionMessagesReceived")
    // Validate transaction TODO : This can be more efficient, calls get repeated several times
    // in event where a new signature is being made by another peer it's most likely still valid, should
    // cache the results of this somewhere.

    // TODO: Move memPool update logic to actor and send response back to EdgeProcessor
    //if (!memPool.transactions.contains(tx)) {
    if (dao.threadSafeTXMemPool.put(tx)) {

      //val pool = memPool.copy(transactions = memPool.transactions + tx)
      //dao.metricsManager ! UpdateMetric("transactionMemPool", pool.transactions.size.toString)
      //dao.threadSafeMemPool.put(transaction)

      dao.metricsManager ! UpdateMetric("transactionMemPool", dao.threadSafeTXMemPool.unsafeCount.toString)
      dao.metricsManager ! IncrementMetric("transactionValidMessages")
      formCheckpoint() // TODO: Send to checkpoint formation actor instead
    } else {
      dao.metricsManager ! IncrementMetric("transactionValidMemPoolDuplicateMessages")
      //memPool
    }

    /*    val finished = Validation.validateTransaction(dao.dbActor, tx)



        finished match {
          // TODO : Increment metrics here for each case
          case t : TransactionValidationStatus if t.validByCurrentStateMemPool =>


            // TODO : Store something here for status queries. Make sure it doesn't cause a conflict

          case t : TransactionValidationStatus =>

            // TODO : Add info somewhere so node can find out transaction was invalid on a callback
            reportInvalidTransaction(dao: DAO, t: TransactionValidationStatus)
            memPool
        }

        */

  }

  def reportInvalidTransaction(dao: DAO, t: TransactionValidationStatus): Unit = {
    dao.metricsManager ! IncrementMetric("invalidTransactions")
    if (t.isDuplicateHash) {
      dao.metricsManager ! IncrementMetric("hashDuplicateTransactions")
    }
    if (!t.sufficientBalance) {
      dao.metricsManager ! IncrementMetric("insufficientBalanceTransactions")
    }
  }

  case class CreateCheckpointEdgeResponse(
                                           checkpointEdge: CheckpointEdge,
                                           transactionsUsed: Set[String],
                                           // filteredValidationTips: Seq[SignedObservationEdge],
                                           updatedTransactionMemPoolThresholdMet: Set[String]
                                         )


  def createCheckpointBlock(transactions: Seq[Transaction], tips: Seq[SignedObservationEdge])
                           (implicit keyPair: KeyPair): CheckpointBlock = {

    val checkpointEdgeData = CheckpointEdgeData(transactions.map{_.hash}.sorted)

    val observationEdge = ObservationEdge(
      TypedEdgeHash(tips.head.hash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(tips(1).hash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash))
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(
      Edge(observationEdge, soe, ResolvedObservationEdge(tips.head, tips(1), Some(checkpointEdgeData)))
    )

    CheckpointBlock(transactions, checkpointEdge)
  }

  def createCheckpointEdgeProposal(
                                    transactionMemPoolThresholdMet: Set[String],
                                    minCheckpointFormationThreshold: Int,
                                    tips: Seq[SignedObservationEdge],
                                  )(implicit keyPair: KeyPair): CreateCheckpointEdgeResponse = {

    val transactionsUsed = transactionMemPoolThresholdMet.take(minCheckpointFormationThreshold)
    val updatedTransactionMemPoolThresholdMet = transactionMemPoolThresholdMet -- transactionsUsed

    val checkpointEdgeData = CheckpointEdgeData(transactionsUsed.toSeq.sorted)

    //val tips = validationTips.take(2)
    //val filteredValidationTips = validationTips.filterNot(tips.contains)

    val observationEdge = ObservationEdge(
      TypedEdgeHash(tips.head.hash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(tips(1).hash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash))
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(Edge(observationEdge, soe, ResolvedObservationEdge(tips.head, tips(1), Some(checkpointEdgeData))))

    CreateCheckpointEdgeResponse(checkpointEdge, transactionsUsed,
      //filteredValidationTips,
      updatedTransactionMemPoolThresholdMet)
  }
  // TODO: Send facilitator selection data (i.e. criteria) as well for verification

  case class SignatureRequest(checkpointBlock: CheckpointBlock, facilitators: Set[Id])
  case class SignatureResponse(checkpointBlock: CheckpointBlock, facilitators: Set[Id], reRegister: Boolean = false)
  case class FinishedCheckpoint(checkpointCacheData: CheckpointCacheData, facilitators: Set[Id])
  case class FinishedHeader(
                             soeCache: SignedObservationEdgeCacheData,
                             facilitators: Set[Id]
                           )
  case class FinishedCheckpointResponse(reRegister: Boolean = false)

  // TODO: Move to checkpoint formation actor
  def formCheckpoint()(implicit dao: DAO): Unit = {

    implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

    val maybeTransactions = dao.threadSafeTXMemPool.pull()

    dao.metricsManager ! IncrementMetric("attemptFormCheckpointCalls")

    if (maybeTransactions.isEmpty) {
      dao.metricsManager ! IncrementMetric("attemptFormCheckpointInsufficientTX")
    }

    maybeTransactions.foreach { transactions =>



      val maybeTips = dao.threadSafeTipService.pull()
      if (maybeTips.isEmpty) {
        dao.metricsManager ! IncrementMetric("attemptFormCheckpointInsufficientTipsOrFacilitators")
      }

      maybeTips.foreach { case (tipData, facils) =>

        val checkpointBlock = createCheckpointBlock(transactions, tipData.map{_.soe})(dao.keyPair)
        dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")

        val finalFacilitators = facils.keySet

        val response = facils.map { case (facilId, data) =>
          facilId -> tryWithMetric( // TODO: onComplete instead, debugging with other changes first.
            {
              data.client.postSync(s"request/signature", SignatureRequest(checkpointBlock, finalFacilitators + dao.id))
            },
            "formCheckpointPOSTCallSignatureRequest"
          )
        }

        if (response.exists(_._2.isFailure)) {

          dao.metricsManager ! IncrementMetric("formCheckpointSignatureResponseMissing")

        } else {

          val nonFailedResponses = response.mapValues(_.get)

          if (!nonFailedResponses.forall(_._2.isSuccess)) {

            dao.metricsManager ! IncrementMetric("formCheckpointSignatureResponseNot2xx")

          } else {

            val parsed = nonFailedResponses.mapValues(v =>
              tryWithMetric({
                v.body.x[Option[SignatureResponse]]
              }, "formCheckpointSignatureResponseJsonParsing")
            )

            if (parsed.exists(_._2.isFailure)) {

              dao.metricsManager ! IncrementMetric("formCheckpointSignatureResponseExistsParsingFailure")

            } else {

              val withValue = parsed.mapValues(_.get)

              if (withValue.exists(_._2.isEmpty)) {

                dao.metricsManager ! IncrementMetric("formCheckpointSignatureResponseEmpty")

              } else {

                // Successful formation
                dao.metricsManager ! IncrementMetric("formCheckpointSignatureResponseAllResponsesPresent")

                val responseValues = withValue.values.map{_.get}

                responseValues.foreach{ sr =>

                  if (sr.reRegister) {
                    // PeerManager.attemptRegisterPeer() TODO : Finish

                  }

                }

                val blocks = responseValues.map {_.checkpointBlock}

                val finalCB = blocks.reduce { (x: CheckpointBlock, y: CheckpointBlock) => x.plus(y) }.plus(checkpointBlock)

                if (finalCB.signatures.size != finalFacilitators.size + 1) {
                  dao.metricsManager ! IncrementMetric("missingBlockSignatures")
                } else {
                  dao.metricsManager ! IncrementMetric("sufficientBlockSignatures")

                  if (!finalCB.simpleValidation()) {

                    dao.metricsManager ! IncrementMetric("finalCBFailedValidation")

                  } else {
                    dao.metricsManager ! IncrementMetric("finalCBPassedValidation")


                    val cache = CheckpointCacheData(
                      Some(finalCB),
                      height = finalCB.calculateHeight(),
                      partition = dao.partition,
                      parentPartitions = tipData.map{_.partition}
                    )

                    dao.threadSafeTipService.accept(cache)
                    // TODO: Check failures and/or remove constraint of single actor
                    dao.peerInfo.foreach { case (id, client) =>

                      if (
                        client.peerMetadata.partition == dao.partition
                      ) {

                        futureTryWithTimeoutMetric(
                          client.client.postSync(s"finished/checkpoint", FinishedCheckpoint(cache, finalFacilitators)),
                          "finishedCheckpointBroadcast",
                          timeoutSeconds = 20
                        )
                        // Send full finished checkpoint

                      } else {

                        val dstPartition = client.peerMetadata.partition
                        val partitionLookup = dao.peerInfo.map{
                          case (k,v) =>
                            k.address.address -> v.peerMetadata.partition
                        }

                        val fh = FinishedHeader(
                          SignedObservationEdgeCacheData(
                            Some(finalCB.soe),
                            height = cache.height,
                            cache.checkpointBlock.get.parentSOEBaseHashes,
                            cache.parentPartitions,
                            dao.partition,
                            finalCB.transactions.filter{
                              t => partitionLookup(t.dst.address) == dstPartition
                            }.map{ t =>
                              t.dst.address -> t.amount
                            }.toMap
                          ),
                          finalFacilitators
                        )

                        futureTryWithTimeoutMetric(
                          client.client.postSync(s"finished/header", fh),
                          "finishedHeaderBroadcast",
                          timeoutSeconds = 20
                        )
                        // Send headers only

                      }

                    }
                  }

                }

              }

            }

          }

        }
      }
    }
  }


  // Temporary for testing join/leave logic.
  def handleSignatureRequest(sr: SignatureRequest)(implicit dao: DAO): SignatureResponse = {
    //if (sr.facilitators.contains(dao.id)) {
    // val replyTo = sr.checkpointBlock.witnessIds.head
    val updated = if (sr.checkpointBlock.simpleValidation()) {
      sr.checkpointBlock.plus(dao.keyPair)
    }
    else {
      sr.checkpointBlock
    }
    SignatureResponse(updated, sr.facilitators)
    /*dao.peerManager ! APIBroadcast(
      _.post(s"response/signature", SignatureResponse(updated, sr.facilitators)),
      peerSubset = Set(replyTo)
    )*/
    // } else None
  }


  def simpleResolveCheckpoint(hash: String, partition: Int)(implicit dao: DAO): Boolean = {

    val peers = dao.readyPeers.values.toSeq.filter(_.peerMetadata.partition == partition)
    var activePeer = peers.head.client
    var remainingPeers : Seq[APIClient] = peers.tail.map{_.client}

    var done = false

    while (!done && remainingPeers.nonEmpty) {

      // TODO: Refactor all the error handling on these to include proper status codes etc.
      // See formCheckpoint for better example of error handling
      val res = tryWithMetric(
        {activePeer.getBlocking[Option[CheckpointCacheData]]("checkpoint/" + hash, timeoutSeconds = 10)},
        "downloadCheckpoint"
      )

      done = res.toOption.exists(_.nonEmpty)

      if (done) {
        val x = res.get.get
        if (!(
          dao.checkpointService.lruCache.contains(x.checkpointBlock.get.baseHash) ||
          dao.soeService.lruCache.contains(x.checkpointBlock.get.baseHash)
          )) {
          dao.metricsManager ! IncrementMetric("resolveAcceptCBCall")
          acceptWithResolveAttempt(x)
        } else {
          dao.metricsManager ! IncrementMetric("resolveCBDuplicate")
        }
      } else {
        if (remainingPeers.nonEmpty) {
          dao.metricsManager ! IncrementMetric("resolvePeerIncrement")
          activePeer = remainingPeers.head
          remainingPeers = remainingPeers.filterNot(_ == activePeer)
        }
      }
    }
    done

  }

  def simpleResolveHeader(hash: String, part: Int)(implicit dao: DAO): Boolean = {

    // TODO: abstract retries and merge with above function
    val peers = dao.readyPeers.values.toSeq
    var activePeer = peers.head.client
    var remainingPeers : Seq[APIClient] = peers.tail.map{_.client}

    var done = false

    while (!done && remainingPeers.nonEmpty) {

      // TODO: Refactor all the error handling on these to include proper status codes etc.
      // See formCheckpoint for better example of error handling
      val res = tryWithMetric(
        {activePeer.getBlocking[Option[SignedObservationEdgeCacheData]]("soe/" + hash, timeoutSeconds = 10)},
        "downloadHeader"
      )

      done = res.toOption.exists(_.nonEmpty)

      if (done) {
        val x = res.get.get
        if (!(
          dao.soeService.lruCache.contains(x.signedObservationEdge.get.baseHash) ||
          dao.checkpointService.lruCache.contains(x.signedObservationEdge.get.baseHash)
          )) {
          dao.metricsManager ! IncrementMetric("resolveAcceptCBCall")
          acceptHeaderWithResolveAttempt(x)
        } else {
          dao.metricsManager ! IncrementMetric("resolveHeaderDuplicate")
        }
      } else {
        if (remainingPeers.nonEmpty) {
          dao.metricsManager ! IncrementMetric("resolvePeerIncrement")
          activePeer = remainingPeers.head
          remainingPeers = remainingPeers.filterNot(_ == activePeer)
        }
      }
    }
    done

  }

  def acceptWithResolveAttempt(checkpointCacheData: CheckpointCacheData)(implicit dao: DAO): Unit = {

    dao.threadSafeTipService.accept(checkpointCacheData)
    val block = checkpointCacheData.checkpointBlock.get
    val parents = block.parentSOEBaseHashes
    val parentExists = parents.zip(checkpointCacheData.parentPartitions).map{ case (h, part) => (h, part) -> {
      dao.checkpointService.lruCache.contains(h) ||
      dao.soeService.lruCache.contains(h)
    }}
    if (parentExists.forall(_._2)) {
      dao.metricsManager ! IncrementMetric("resolveFinishedCheckpointParentsPresent")
    } else {
      dao.metricsManager ! IncrementMetric("resolveFinishedCheckpointParentMissing")
      parentExists.filterNot(_._2).foreach{
        case ((h, part), _ ) =>
          futureTryWithTimeoutMetric(
            simpleResolveCheckpoint(h, part),
            "resolveCheckpoint",
            timeoutSeconds = 30
          )(dao.edgeExecutionContext, dao)
      }

    }

  }

  def resolveParent(h: String, part: Int)(implicit dao: DAO): Unit = {
    if (part == dao.partition) {
      futureTryWithTimeoutMetric(
        simpleResolveCheckpoint(h, part),
        "resolveCheckpoint",
        timeoutSeconds = 30
      )(dao.edgeExecutionContext, dao)
    } else {
      futureTryWithTimeoutMetric(
        simpleResolveHeader(h, part),
        "resolveHeader",
        timeoutSeconds = 30
      )(dao.edgeExecutionContext, dao)
    }
  }

  def acceptHeaderWithResolveAttempt(soeCache: SignedObservationEdgeCacheData)(implicit dao: DAO): Unit = {
    dao.threadSafeTipService.accept(soeCache)
    val parents = soeCache.parentSOEBaseHashes
    val parentExists = parents.zip(soeCache.parentPartitions).map{case (h, part) => (h, part) -> {
      dao.checkpointService.lruCache.contains(h) ||
        dao.soeService.lruCache.contains(h)
    }}

    if (parentExists.forall(_._2)) {
      dao.metricsManager ! IncrementMetric("resolveFinishedHeaderParentsPresent")
    } else {
      dao.metricsManager ! IncrementMetric("resolveFinishedHeaderParentMissing")
      parentExists.filterNot(_._2).foreach{
        case ((h, part), _ ) =>
         resolveParent(h, part)
      }

    }

  }


  def handleFinishedHeader(fh: FinishedHeader)(implicit dao: DAO): Future[Try[Any]] = {
    futureTryWithTimeoutMetric(
      if (dao.nodeState == NodeState.DownloadCompleteAwaitingFinalSync) {
        dao.threadSafeTipService.syncBufferAccept(fh.soeCache)
        Future.successful()
      } else if (dao.nodeState == NodeState.Ready) {
        if (fh.soeCache.signedObservationEdge.exists {
          _.simpleValidation()
        }) {
          acceptHeaderWithResolveAttempt(fh.soeCache)
        } else Future.successful()
      } else Future.successful()
      , "handleFinishedHeader"
    )(dao.finishedExecutionContext, dao)
  }


  def handleFinishedCheckpoint(fc: FinishedCheckpoint)(implicit dao: DAO): Future[Try[Any]] = {
    futureTryWithTimeoutMetric(
      if (dao.nodeState == NodeState.DownloadCompleteAwaitingFinalSync) {
        dao.threadSafeTipService.syncBufferAccept(fc.checkpointCacheData)
        Future.successful()
      } else if (dao.nodeState == NodeState.Ready) {
        if (fc.checkpointCacheData.checkpointBlock.exists {
          _.simpleValidation()
        }) {
          acceptWithResolveAttempt(fc.checkpointCacheData)
        } else Future.successful()
      } else Future.successful()
      , "handleFinishedCheckpoint"
    )(dao.finishedExecutionContext, dao)
  }


}

case class TipData(soe: SignedObservationEdge, numUses: Int, partition: Int)

case class SnapshotInfo(
                         snapshot: Snapshot,
                         acceptedCBSinceSnapshot: Seq[String] = Seq(),
                         acceptedHeadersSinceSnapshot: Seq[String] = Seq(),
                         acceptedCBSinceSnapshotCache: Seq[CheckpointCacheData] = Seq(),
                         acceptedHeadersSinceSnapshotCache: Seq[SignedObservationEdgeCacheData] = Seq(),
                         lastSnapshotHeight: Int = 0,
                         snapshotHashes: Seq[String] = Seq(),
                         addressCacheData: Map[String, AddressCacheData] = Map(),
                         tips: Map[String, TipData] = Map(),
                         snapshotCache: Seq[CheckpointCacheData] = Seq()
                       )

case object GetMemPool

case class Snapshot(lastSnapshot: String, checkpointBlocks: Seq[String]) extends ProductHash
case class StoredSnapshot(
                           snapshot: Snapshot,
                           checkpointCache: Seq[CheckpointCacheData],
                           partition: Int = 0, // Temporary
                           soeHeaders: Seq[SignedObservationEdgeCacheData] = Seq()
                         )

case class DownloadComplete(latestSnapshot: Snapshot)

object Snapshot {

  def triggerSnapshot(round: Long)(implicit dao: DAO): Unit = {
    // TODO: Refactor round into InternalHeartbeat
    if (round % dao.processingConfig.snapshotInterval == 0 && dao.nodeState == NodeState.Ready) {
      futureTryWithTimeoutMetric(
        dao.threadSafeTipService.attemptSnapshot(), "snapshotAttempt"
      )(dao.edgeExecutionContext, dao)
    }
  }



  val snapshotZero = Snapshot("", Seq())
  val snapshotZeroHash: String = Snapshot("", Seq()).hash

  def acceptSnapshot(snapshot: Snapshot)(implicit dao: DAO): Unit = {
    // dao.dbActor.putSnapshot(snapshot.hash, snapshot)
    val cbData = snapshot.checkpointBlocks.map{dao.checkpointService.get}
    val header = snapshot.checkpointBlocks.flatMap{dao.soeService.get}
/*    if (cbData.exists{_.isEmpty}) {
      dao.metricsManager ! IncrementMetric("snapshotCBAcceptQueryFailed")
    }*/

    for (
      cbOpt <- cbData;
      cbCache <- cbOpt;
      cb <- cbCache.checkpointBlock;
      tx <- cb.transactions
    ) {
      // TODO: Should really apply this to the N-1 snapshot instead of doing it directly
      // To allow consensus more time since the latest snapshot includes all data up to present, but this is simple for now
      tx.ledgerApplySnapshot()
      dao.transactionService.delete(Set(tx.hash))
      dao.metricsManager ! IncrementMetric("snapshotAppliedBalance")
    }

    for (
      h <- header;
      (dst, amount) <- h.diffMap
    )  {
      dao.addressService.update(
        dst,
        { a: AddressCacheData => a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot + amount)},
        AddressCacheData(0L, 0L) // unused since this address should already exist here
      )
    }


  }


}


