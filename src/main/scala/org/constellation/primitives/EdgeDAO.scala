package org.constellation.primitives

import java.util.concurrent.{Executors, Semaphore, TimeUnit}

import akka.util.Timeout
import org.constellation.consensus.EdgeProcessor.acceptCheckpoint
import org.constellation.consensus._
import org.constellation.primitives.Schema._
import org.constellation.primitives.storage._
import org.constellation.{DAO, ProcessingConfig}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Random

class ThreadSafeTXMemPool() {

  private var transactions = Seq[Transaction]()

  def pull(minCount: Int): Option[Seq[Transaction]] = this.synchronized {
    if (transactions.size > minCount) {
      val (left, right) = transactions.splitAt(minCount)
      transactions = right
      Some(left)
    } else None
  }

  def batchPutDebug(txs: Seq[Transaction]): Boolean = this.synchronized {
    transactions ++= txs
    true
  }

  def put(transaction: Transaction, overrideLimit: Boolean = false)(implicit dao: DAO): Boolean =
    this.synchronized {
      val notContained = !transactions.contains(transaction)

      if (notContained) {
        if (overrideLimit) {
          // Prepend in front to process user TX first before random ones
          transactions = Seq(transaction) ++ transactions

        } else if (transactions.size < dao.processingConfig.maxMemPoolSize) {
          transactions :+= transaction
        }
      }
      notContained
    }

  def unsafeCount: Int = transactions.size

}

class ThreadSafeMessageMemPool() {

  private var messages = Seq[Seq[ChannelMessage]]()

  val activeChannels: TrieMap[String, Semaphore] = TrieMap()

  val messageHashToSendRequest: TrieMap[String, ChannelSendRequest] = TrieMap()

  def release(messages: Seq[ChannelMessage]): Unit = {
    messages.foreach { m =>
      activeChannels(m.signedMessageData.data.channelId)
        .release()
    }
  }

  def pull(minCount: Int): Option[Seq[ChannelMessage]] = this.synchronized {
    if (messages.size > minCount) {
      val (left, right) = messages.splitAt(minCount)
      messages = right
      Some(left.flatten)
    } else None
  }

  def batchPutDebug(messagesToAdd: Seq[ChannelMessage]): Boolean = this.synchronized {
    //messages ++= messagesToAdd
    true
  }

  def put(message: Seq[ChannelMessage],
          overrideLimit: Boolean = false)(implicit dao: DAO): Boolean = this.synchronized {
    val notContained = !messages.contains(message)

    if (notContained) {
      if (overrideLimit) {
        // Prepend in front to process user TX first before random ones
        messages = Seq(message) ++ messages

      } else if (messages.size < dao.processingConfig.maxMemPoolSize) {
        messages :+= message
      }
    }
    notContained
  }

  def unsafeCount: Int = messages.size

}

import constellation._

class ThreadSafeTipService() {

  implicit val timeout: Timeout = Timeout(15, TimeUnit.SECONDS)

  private var thresholdMetCheckpoints: Map[String, TipData] = Map()
  var acceptedCBSinceSnapshot: Seq[String] = Seq()
  var facilitators: Map[Id, PeerData] = Map()
  private var snapshot: Snapshot = Snapshot.snapshotZero

  def tips: Map[String, TipData] = thresholdMetCheckpoints

  def getSnapshotInfo()(implicit dao: DAO): SnapshotInfo = this.synchronized(
    SnapshotInfo(
      snapshot,
      acceptedCBSinceSnapshot,
      lastSnapshotHeight = lastSnapshotHeight,
      snapshotHashes = dao.snapshotHashes,
      addressCacheData = dao.addressService.toMap(),
      tips = thresholdMetCheckpoints,
      snapshotCache = snapshot.checkpointBlocks.flatMap { dao.checkpointService.get }
    )
  )

  var totalNumCBsInShapshots = 0L

  // ONLY TO BE USED BY DOWNLOAD COMPLETION CALLER

  def setSnapshot(latestSnapshotInfo: SnapshotInfo)(implicit dao: DAO): Unit = this.synchronized {
    snapshot = latestSnapshotInfo.snapshot
    lastSnapshotHeight = latestSnapshotInfo.lastSnapshotHeight
    thresholdMetCheckpoints = latestSnapshotInfo.tips

    // Below may not be necessary, just a sanity check
    acceptedCBSinceSnapshot = latestSnapshotInfo.acceptedCBSinceSnapshot
    latestSnapshotInfo.addressCacheData.foreach {
      case (k, v) =>
        dao.addressService.put(k, v)
    }

    latestSnapshotInfo.snapshotCache.foreach { h =>
      dao.metrics.incrementMetric("checkpointAccepted")
      dao.checkpointService.put(h.checkpointBlock.get.baseHash, h)
      h.checkpointBlock.get.storeSOE()
      h.checkpointBlock.get.transactions.foreach { _ =>
        dao.metrics.incrementMetric("transactionAccepted")
      }
    }

    latestSnapshotInfo.acceptedCBSinceSnapshotCache.foreach { h =>
      dao.checkpointService.put(h.checkpointBlock.get.baseHash, h)
      h.checkpointBlock.get.storeSOE()
      dao.metrics.incrementMetric("checkpointAccepted")
      h.checkpointBlock.get.transactions.foreach { _ =>
        dao.metrics.incrementMetric("transactionAccepted")
      }
    }

    dao.metrics.updateMetric(
      "acceptCBCacheMatchesAcceptedSize",
      (latestSnapshotInfo.acceptedCBSinceSnapshot.size == latestSnapshotInfo.acceptedCBSinceSnapshotCache.size).toString
    )

  }

  // TODO: Read from lastSnapshot in DB optionally, assign elsewhere
  var lastSnapshotHeight = 0

  def getMinTipHeight()(implicit dao: DAO) =
    thresholdMetCheckpoints.keys
      .map {
        dao.checkpointService.get
      }
      .flatMap {
        _.flatMap {
          _.height.map {
            _.min
          }
        }
      }
      .min

  var syncBuffer: Seq[CheckpointCacheData] = Seq()

  def syncBufferAccept(cb: CheckpointCacheData)(implicit dao: DAO): Unit = {
    syncBuffer :+= cb
    dao.metrics.updateMetric("syncBufferSize", syncBuffer.size.toString)
  }

  def attemptSnapshot()(implicit dao: DAO): Unit = this.synchronized {

    // Sanity check memory protection
    if (thresholdMetCheckpoints.size > dao.processingConfig.maxActiveTipsAllowedInMemory) {
      thresholdMetCheckpoints = thresholdMetCheckpoints.slice(0, 100)
      dao.metrics.incrementMetric("memoryExceeded_thresholdMetCheckpoints")
      dao.metrics.updateMetric("activeTips", thresholdMetCheckpoints.size.toString)
    }
    if (acceptedCBSinceSnapshot.size > dao.processingConfig.maxAcceptedCBHashesInMemory) {
      acceptedCBSinceSnapshot = acceptedCBSinceSnapshot.slice(0, 100)
      dao.metrics.incrementMetric("memoryExceeded_acceptedCBSinceSnapshot")
      dao.metrics.updateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size.toString)
    }

    val peerIds = dao.peerInfo //(dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq
    val facilMap = peerIds.filter {
      case (_, pd) =>
        pd.peerMetadata.timeAdded < (System
          .currentTimeMillis() - dao.processingConfig.minPeerTimeAddedSeconds * 1000) && pd.peerMetadata.nodeState == NodeState.Ready
    }

    facilitators = facilMap

    if (dao.nodeState == NodeState.Ready && acceptedCBSinceSnapshot.nonEmpty) {

      val minTipHeight = getMinTipHeight()
      dao.metrics.updateMetric("minTipHeight", minTipHeight.toString)

      val nextHeightInterval = lastSnapshotHeight + dao.processingConfig.snapshotHeightInterval

      val canSnapshot = minTipHeight > (nextHeightInterval + dao.processingConfig.snapshotHeightDelayInterval)
      if (!canSnapshot) {
        dao.metrics.incrementMetric("snapshotHeightIntervalConditionNotMet")
      } else {

        val maybeDatas = acceptedCBSinceSnapshot.map(dao.checkpointService.get)

        val blocksWithinHeightInterval = maybeDatas.filter {
          _.exists(_.height.exists { h =>
            h.min > lastSnapshotHeight && h.min <= nextHeightInterval
          })
        }

        if (blocksWithinHeightInterval.isEmpty) {
          dao.metrics.incrementMetric("snapshotNoBlocksWithinHeightInterval")
        } else {

          val blockCaches = blocksWithinHeightInterval.map {
            _.get
          }

          val hashesForNextSnapshot = blockCaches.map {
            _.checkpointBlock.get.baseHash
          }.sorted
          val nextSnapshot = Snapshot(snapshot.hash, hashesForNextSnapshot)

          // TODO: Make this a future and have it not break the unit test
          // Also make the db puts blocking, may help for different issue
          if (snapshot != Snapshot.snapshotZero) {
            dao.metrics.incrementMetric("snapshotCount")

            // Write snapshot to file
            tryWithMetric(
              {
                val maybeBlocks = snapshot.checkpointBlocks.map {
                  dao.checkpointService.get
                }
                if (maybeBlocks.exists(_.exists(_.checkpointBlock.isEmpty))) {
                  // TODO : This should never happen, if it does we need to reset the node state and redownload
                  dao.metrics.incrementMetric("snapshotWriteToDiskMissingData")
                }
                val flatten = maybeBlocks.flatten.sortBy(_.checkpointBlock.map {
                  _.baseHash
                })
                Snapshot.writeSnapshot(StoredSnapshot(snapshot, flatten))
                // dao.dbActor.kvdb.put("latestSnapshot", snapshot)
              },
              "snapshotWriteToDisk"
            )

            Snapshot.acceptSnapshot(snapshot)
            dao.checkpointService.delete(snapshot.checkpointBlocks.toSet)

            totalNumCBsInShapshots += snapshot.checkpointBlocks.size
            dao.metrics.updateMetric("totalNumCBsInShapshots", totalNumCBsInShapshots.toString)
            dao.metrics.updateMetric("lastSnapshotHash", snapshot.hash)
          }

          // TODO: Verify from file
          /*
        if (snapshot.lastSnapshot != Snapshot.snapshotZeroHash && snapshot.lastSnapshot != "") {

          val lastSnapshotVerification = File(dao.snapshotPath, snapshot.lastSnapshot).read
          if (lastSnapshotVerification.isEmpty) {
            dao.metrics.incrementMetric("snapshotVerificationFailed")
          } else {
            dao.metrics.incrementMetric("snapshotVerificationCount")
            if (
              !lastSnapshotVerification.get.checkpointBlocks.map {
                dao.checkpointService.get
              }.forall(_.exists(_.checkpointBlock.nonEmpty))
            ) {
              dao.metrics.incrementMetric("snapshotCBVerificationFailed")
            } else {
              dao.metrics.incrementMetric("snapshotCBVerificationCount")
            }

          }
        }
           */

          lastSnapshotHeight = nextHeightInterval
          snapshot = nextSnapshot
          acceptedCBSinceSnapshot =
            acceptedCBSinceSnapshot.filterNot(hashesForNextSnapshot.contains)
          dao.metrics.updateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size.toString)
          dao.metrics.updateMetric("lastSnapshotHeight", lastSnapshotHeight.toString)
          dao.metrics.updateMetric(
            "nextSnapshotHeight",
            (lastSnapshotHeight + dao.processingConfig.snapshotHeightInterval).toString
          )
        }
      }
    }
  }

  def acceptGenesis(genesisObservation: GenesisObservation): Unit = this.synchronized {
    thresholdMetCheckpoints += genesisObservation.initialDistribution.baseHash -> TipData(
      genesisObservation.initialDistribution,
      0
    )
    thresholdMetCheckpoints += genesisObservation.initialDistribution2.baseHash -> TipData(
      genesisObservation.initialDistribution2,
      0
    )
  }

  def pull(
    allowEmptyFacilitators: Boolean = false
  )(implicit dao: DAO): Option[(Seq[SignedObservationEdge], Map[Id, PeerData])] =
    this.synchronized {
      val res =
        if (thresholdMetCheckpoints.size >= 2 && (facilitators.nonEmpty || allowEmptyFacilitators)) {
          val tips = Random.shuffle(thresholdMetCheckpoints.toSeq).take(2)

          val tipSOE = tips
            .map {
              _._2.checkpointBlock.checkpoint.edge.signedObservationEdge
            }
            .sortBy(_.hash)

          val mergedTipHash = tipSOE.map { _.hash }.mkString("")

          val totalNumFacil = facilitators.size

          val finalFacilitators = if (totalNumFacil > 0) {
            // TODO: Use XOR distance instead as it handles peer data mismatch cases better
            val facilitatorIndex = (BigInt(mergedTipHash, 16) % totalNumFacil).toInt
            val sortedFacils = facilitators.toSeq.sortBy(_._1.hex)
            val selectedFacils = Seq
              .tabulate(dao.processingConfig.numFacilitatorPeers) { i =>
                (i + facilitatorIndex) % totalNumFacil
              }
              .map {
                sortedFacils(_)
              }
            selectedFacils.toMap
          } else {
            Map[Id, PeerData]()
          }

          Some(tipSOE -> finalFacilitators)
        } else None

      dao.metrics.updateMetric("activeTips", thresholdMetCheckpoints.size.toString)
      res
    }

  // TODO: Synchronize only on values modified by this, same for other functions

  def accept(checkpointCacheData: CheckpointCacheData)(implicit dao: DAO): Unit =
    this.synchronized {

      if (dao.checkpointService.contains(
            checkpointCacheData.checkpointBlock
              .map {
                _.baseHash
              }
              .getOrElse("")
          )) {

        dao.metrics.incrementMetric("checkpointAcceptBlockAlreadyStored")

      } else {

        tryWithMetric(acceptCheckpoint(checkpointCacheData), "acceptCheckpoint")

        def reuseTips: Boolean = thresholdMetCheckpoints.size < dao.maxWidth

        checkpointCacheData.checkpointBlock.foreach { checkpointBlock =>
          val keysToRemove = checkpointBlock.parentSOEBaseHashes.flatMap { h =>
            thresholdMetCheckpoints.get(h).flatMap {
              case TipData(block, numUses) =>
                def doRemove(): Option[String] = {
                  dao.metrics.incrementMetric("checkpointTipsRemoved")
                  Some(block.baseHash)
                }

                if (reuseTips) {
                  if (numUses >= 2) {
                    doRemove()
                  } else {
                    None
                  }
                } else {
                  doRemove()
                }
            }
          }

          val keysToUpdate = checkpointBlock.parentSOEBaseHashes.flatMap { h =>
            thresholdMetCheckpoints.get(h).flatMap {
              case TipData(block, numUses) =>
                def doUpdate(): Option[(String, TipData)] = {
                  dao.metrics.incrementMetric("checkpointTipsIncremented")
                  Some(block.baseHash -> TipData(block, numUses + 1))
                }

                if (reuseTips && numUses <= 2) {
                  doUpdate()
                } else None
            }
          }.toMap

          thresholdMetCheckpoints = thresholdMetCheckpoints +
            (checkpointBlock.baseHash -> TipData(checkpointBlock, 0)) ++
            keysToUpdate --
            keysToRemove

          if (acceptedCBSinceSnapshot.contains(checkpointBlock.baseHash)) {
            dao.metrics.incrementMetric("checkpointAcceptedButAlreadyInAcceptedCBSinceSnapshot")
          } else {
            acceptedCBSinceSnapshot = acceptedCBSinceSnapshot :+ checkpointBlock.baseHash
            dao.metrics.updateMetric("acceptedCBSinceSnapshot",
                                     acceptedCBSinceSnapshot.size.toString)
          }

        }
      }
    }

}

trait EdgeDAO {

  var processingConfig = ProcessingConfig()

  @volatile var blockFormationInProgress: Boolean = false

  val publicReputation: TrieMap[Id, Double] = TrieMap()
  val secretReputation: TrieMap[Id, Double] = TrieMap()

  val otherNodeScores: TrieMap[Id, TrieMap[Id, Double]] = TrieMap()

  val checkpointService = new CheckpointService(processingConfig.checkpointLRUMaxSize)
  val transactionService = new TransactionService(processingConfig.transactionLRUMaxSize)
  val addressService = new AddressService(processingConfig.addressLRUMaxSize)
  val messageService = new MessageService()
  val soeService = new SOEService()

  val threadSafeTXMemPool = new ThreadSafeTXMemPool()
  val threadSafeMessageMemPool = new ThreadSafeMessageMemPool()
  val threadSafeTipService = new ThreadSafeTipService()

  var genesisBlock: Option[CheckpointBlock] = None
  var genesisObservation: Option[GenesisObservation] = None

  def maxWidth: Int = processingConfig.maxWidth

  def minCheckpointFormationThreshold: Int = processingConfig.minCheckpointFormationThreshold

  def minCBSignatureThreshold: Int = processingConfig.numFacilitatorPeers

  val resolveNotifierCallbacks: TrieMap[String, Seq[CheckpointBlock]] = TrieMap()

  val edgeExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

  // val peerAPIExecutionContext: ExecutionContextExecutor =
  //   ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

  val apiClientExecutionContext: ExecutionContextExecutor = edgeExecutionContext
  //  ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

  val signatureExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

  val finishedExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(40))

  // Temporary to get peer data for tx hash partitioning
  @volatile var peerInfo: Map[Id, PeerData] = Map()

  def readyPeers: Map[Id, PeerData] =
    peerInfo.filter(_._2.peerMetadata.nodeState == NodeState.Ready)

}
