package org.constellation.primitives

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, TimeUnit}

import akka.util.Timeout
import better.files.File
import com.twitter.storehaus.cache.MutableLRUCache
import org.constellation.consensus.EdgeProcessor.acceptCheckpoint
import org.constellation.consensus._
import org.constellation.primitives.Schema._
import org.constellation.serializer.KryoSerializer
import org.constellation.{DAO, ProcessingConfig}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Random


class ThreadSafeTXMemPool() {

  private var transactions = Seq[Transaction]()

  def pull()(implicit dao: DAO): Option[Seq[Transaction]] = this.synchronized{
    val minCount = dao.minCheckpointFormationThreshold
    if (transactions.size > minCount) {
      val (left, right) = transactions.splitAt(minCount)
      transactions = right
      Some(left)
    } else None
  }

  def batchPutDebug(txs: Seq[Transaction]) : Boolean = this.synchronized{
    transactions ++= txs
    true
  }

  def put(transaction: Transaction, overrideLimit: Boolean = false)(implicit dao: DAO): Boolean = this.synchronized{
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

import constellation._

class ThreadSafeTipService() {

  implicit val timeout: Timeout = Timeout(15, TimeUnit.SECONDS)


  private var thresholdMetCheckpoints: Map[String, TipData] = Map()
  private var acceptedCBSinceSnapshot: Seq[String] = Seq()
  private var acceptedHeadersSinceSnapshot: Seq[String] = Seq()
  private var facilitators: Map[Id, PeerData] = Map()
  private var snapshot: Snapshot = Snapshot.snapshotZero

  def tips: Map[String, TipData] = thresholdMetCheckpoints

  def getSnapshotInfo()(implicit dao: DAO): SnapshotInfo = this.synchronized(
    SnapshotInfo(
      snapshot,
      acceptedCBSinceSnapshot = acceptedCBSinceSnapshot,
      lastSnapshotHeight = lastSnapshotHeight,
      addressCacheData = dao.addressService.lruCache.iterator.toMap,
      tips = thresholdMetCheckpoints,
      snapshotHashes = dao.snapshotHashes,
      snapshotCache = snapshot.checkpointBlocks.flatMap{dao.checkpointService.get},
      acceptedHeadersSinceSnapshot = acceptedHeadersSinceSnapshot,
      acceptedHeadersSinceSnapshotCache = acceptedHeadersSinceSnapshot.flatMap{dao.soeService.get},
      acceptedCBSinceSnapshotCache = acceptedCBSinceSnapshot.flatMap{dao.checkpointService.get}
    )
  )

  var totalNumCBsInShapshots = 0L

  // ONLY TO BE USED BY DOWNLOAD COMPLETION CALLER
  def setSnapshot(latestSnapshotInfo: SnapshotInfo)(implicit dao: DAO): Unit = this.synchronized{
    snapshot = latestSnapshotInfo.snapshot
    lastSnapshotHeight = latestSnapshotInfo.lastSnapshotHeight
    thresholdMetCheckpoints = latestSnapshotInfo.tips

    // Below may not be necessary, just a sanity check
    acceptedCBSinceSnapshot = latestSnapshotInfo.acceptedCBSinceSnapshot
    latestSnapshotInfo.addressCacheData.foreach{
      case (k,v) =>
        dao.addressService.put(k, v)
    }

    latestSnapshotInfo.snapshotCache.foreach{
      h =>
        dao.metricsManager ! IncrementMetric("checkpointAccepted")
        dao.checkpointService.put(h.checkpointBlock.get.baseHash, h)
        h.checkpointBlock.get.transactions.foreach{
          _ =>
            dao.metricsManager ! IncrementMetric("transactionAccepted")
        }
    }

    latestSnapshotInfo.acceptedCBSinceSnapshotCache.foreach{
      h =>
        dao.checkpointService.put(h.checkpointBlock.get.baseHash, h)
        dao.metricsManager ! IncrementMetric("checkpointAccepted")
        h.checkpointBlock.get.transactions.foreach{
          _ =>
            dao.metricsManager ! IncrementMetric("transactionAccepted")
        }
    }

    dao.metricsManager ! UpdateMetric(
      "acceptCBCacheMatchesAcceptedSize",
      (latestSnapshotInfo.acceptedCBSinceSnapshot.size ==
        latestSnapshotInfo.acceptedCBSinceSnapshotCache.size).toString
    )


  }

  // TODO: Read from lastSnapshot in DB optionally, assign elsewhere
  var lastSnapshotHeight = 0

  def getMinTipHeight()(implicit dao: DAO): Long = {

    val cbMins = thresholdMetCheckpoints.keys.map {
      dao.checkpointService.get
    }.flatMap {
      _.flatMap {
        _.height.map {
          _.min
        }
      }
    }
    val headerMins = thresholdMetCheckpoints.keys.map {
      dao.soeService.get
    }.flatMap {
      _.flatMap {
        _.height.map {
          _.min
        }
      }
    }

    Seq(
      if (cbMins.isEmpty) Long.MaxValue else cbMins.min,
      if (headerMins.isEmpty) Long.MaxValue else headerMins.min
    ).min
  }

  var syncBuffer : Seq[CheckpointCacheData] = Seq()
  var headerSyncBuffer : Seq[SignedObservationEdgeCacheData] = Seq()

  def syncBufferAccept(cb: CheckpointCacheData)(implicit dao: DAO): Unit = {
    syncBuffer :+= cb
    dao.metricsManager ! UpdateMetric("syncBufferSize", syncBuffer.size.toString)
  }

  def syncBufferAccept(cb: SignedObservationEdgeCacheData)(implicit dao: DAO): Unit = {
    headerSyncBuffer :+= cb
    dao.metricsManager ! UpdateMetric("headerSyncBuffer", headerSyncBuffer.size.toString)
  }

  def attemptSnapshot()(implicit dao: DAO): Unit = this.synchronized{

    // Sanity check memory protection
    if (thresholdMetCheckpoints.size > dao.processingConfig.maxActiveTipsAllowedInMemory) {
      thresholdMetCheckpoints = thresholdMetCheckpoints.slice(0, 100)
      dao.metricsManager ! IncrementMetric("memoryExceeded_thresholdMetCheckpoints")
      dao.metricsManager ! UpdateMetric("activeTips", thresholdMetCheckpoints.size.toString)
    }
    if (acceptedCBSinceSnapshot.size > dao.processingConfig.maxAcceptedCBHashesInMemory) {
      acceptedCBSinceSnapshot = acceptedCBSinceSnapshot.slice(0, 100)
      dao.metricsManager ! IncrementMetric("memoryExceeded_acceptedCBSinceSnapshot")
      dao.metricsManager ! UpdateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size.toString)
    }


    val peerIds = dao.peerInfo //(dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq
    val facilMap = peerIds.filter{case (_, pd) =>
      pd.peerMetadata.timeAdded < (System.currentTimeMillis() - dao.processingConfig.minPeerTimeAddedSeconds * 1000) && pd.peerMetadata.nodeState == NodeState.Ready
    }

    facilitators = facilMap

    if (dao.nodeState == NodeState.Ready && acceptedCBSinceSnapshot.nonEmpty) {

      val minTipHeight = getMinTipHeight()
      dao.metricsManager ! UpdateMetric("minTipHeight", minTipHeight.toString)

      val nextHeightInterval = lastSnapshotHeight + dao.processingConfig.snapshotHeightInterval

      val canSnapshot = minTipHeight > (nextHeightInterval + dao.processingConfig.snapshotHeightDelayInterval)
      if (!canSnapshot) {
        dao.metricsManager ! IncrementMetric("snapshotHeightIntervalConditionNotMet")
      } else {

        val maybeDatas = acceptedCBSinceSnapshot.map(dao.checkpointService.get)
        val headers = acceptedHeadersSinceSnapshot.flatMap(dao.soeService.get)
        val headersInInterval = headers.filter{
          _.height.exists { h =>
            h.min > lastSnapshotHeight && h.min <= nextHeightInterval
          }
        }

        val blocksWithinHeightInterval = maybeDatas.filter {
          _.exists(_.height.exists { h =>
            h.min > lastSnapshotHeight && h.min <= nextHeightInterval
          })
        }

        if (blocksWithinHeightInterval.isEmpty) {
          dao.metricsManager ! IncrementMetric("snapshotNoBlocksWithinHeightInterval")
        } else {

          val blockCaches = blocksWithinHeightInterval.map {
            _.get
          }

          val cbHashes = blockCaches.map {
            _.checkpointBlock.get.baseHash
          }
          val headerHashes = headersInInterval.flatMap(_.signedObservationEdge.map {
            _.baseHash
          })
          val hashes = cbHashes ++ headerHashes
          val hashesForNextSnapshot = hashes.sorted


          val nextSnapshot = Snapshot(snapshot.hash, hashesForNextSnapshot)

          // TODO: Make this a future and have it not break the unit test
          // Also make the db puts blocking, may help for different issue
          if (snapshot != Snapshot.snapshotZero) {
            dao.metricsManager ! IncrementMetric("snapshotCount")

            // Write snapshot to file
            tryWithMetric({
              val maybeBlocks = snapshot.checkpointBlocks.map {
                dao.checkpointService.get
              }
              /*              if (maybeBlocks.exists(_.exists(_.checkpointBlock.isEmpty))) {
                              // TODO : This should never happen, if it does we need to reset the node state and redownload
                              dao.metricsManager ! IncrementMetric("snapshotWriteToDiskMissingData")
                            }*/
              val flatten = maybeBlocks.flatten.sortBy(_.checkpointBlock.map {
                _.baseHash
              })

              val storeHeaders = snapshot.checkpointBlocks.flatMap{dao.soeService.get}.sortBy(_.signedObservationEdge.get.baseHash)

              File(dao.snapshotPath, snapshot.hash).writeByteArray(KryoSerializer.serializeAnyRef(
                StoredSnapshot(snapshot, flatten, dao.partition, storeHeaders)
              ))
              // dao.dbActor.kvdb.put("latestSnapshot", snapshot)
            },
              "snapshotWriteToDisk"
            )

            Snapshot.acceptSnapshot(snapshot)
            dao.checkpointService.delete(snapshot.checkpointBlocks.toSet)
            dao.soeService.delete(snapshot.checkpointBlocks.toSet)


            totalNumCBsInShapshots += snapshot.checkpointBlocks.size
            dao.metricsManager ! UpdateMetric("totalNumCBsInShapshots", totalNumCBsInShapshots.toString)
            dao.metricsManager ! UpdateMetric("lastSnapshotHash", snapshot.hash)
          }

          // TODO: Verify from file
          /*
        if (snapshot.lastSnapshot != Snapshot.snapshotZeroHash && snapshot.lastSnapshot != "") {

          val lastSnapshotVerification = File(dao.snapshotPath, snapshot.lastSnapshot).read
          if (lastSnapshotVerification.isEmpty) {
            dao.metricsManager ! IncrementMetric("snapshotVerificationFailed")
          } else {
            dao.metricsManager ! IncrementMetric("snapshotVerificationCount")
            if (
              !lastSnapshotVerification.get.checkpointBlocks.map {
                dao.checkpointService.get
              }.forall(_.exists(_.checkpointBlock.nonEmpty))
            ) {
              dao.metricsManager ! IncrementMetric("snapshotCBVerificationFailed")
            } else {
              dao.metricsManager ! IncrementMetric("snapshotCBVerificationCount")
            }

          }
        }
*/

          lastSnapshotHeight = nextHeightInterval
          snapshot = nextSnapshot
          acceptedCBSinceSnapshot = acceptedCBSinceSnapshot.filterNot(hashesForNextSnapshot.contains)
          acceptedHeadersSinceSnapshot = acceptedHeadersSinceSnapshot.filterNot(hashesForNextSnapshot.contains)
          dao.metricsManager ! UpdateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size.toString)
          dao.metricsManager ! UpdateMetric("acceptedHeadersSinceSnapshot", acceptedHeadersSinceSnapshot.size.toString)
          dao.metricsManager ! UpdateMetric("lastSnapshotHeight", lastSnapshotHeight.toString)
          dao.metricsManager ! UpdateMetric("nextSnapshotHeight", (lastSnapshotHeight + dao.processingConfig.snapshotHeightInterval).toString)
        }
      }
    }
  }

  def acceptGenesis(genesisObservation: GenesisObservation): Unit = this.synchronized{
    thresholdMetCheckpoints += genesisObservation.initialDistribution.baseHash -> TipData(genesisObservation.initialDistribution.soe, 0, 0)
    thresholdMetCheckpoints += genesisObservation.initialDistribution2.baseHash -> TipData(genesisObservation.initialDistribution2.soe, 0, 0)
  }

  def pull()(implicit dao: DAO): Option[(Seq[TipData], Map[Id, PeerData])] = this.synchronized{
    if (thresholdMetCheckpoints.size >= 2 && facilitators.nonEmpty) {
      val tips = Random.shuffle(thresholdMetCheckpoints.toSeq).take(2)

      val tipSOE = tips.map {
        _._2
      }.sortBy(_.soe.hash)

      val mergedTipHash = tipSOE.map {_.soe.hash}.mkString("")

      val facilitatorsInPartition = facilitators.filter{_._2.peerMetadata.partition == dao.partition}

      val totalNumFacil = facilitatorsInPartition.size
      // TODO: Use XOR distance instead as it handles peer data mismatch cases better
      val facilitatorIndex = (BigInt(mergedTipHash, 16) % totalNumFacil).toInt
      val sortedFacils = facilitatorsInPartition.toSeq.sortBy(_._1.encodedId.b58Encoded)
      val selectedFacils = Seq.tabulate(dao.processingConfig.numFacilitatorPeers) { i => (i + facilitatorIndex) % totalNumFacil }.map {
        sortedFacils(_)
      }
      val finalFacilitators = selectedFacils.toMap
      dao.metricsManager ! UpdateMetric("activeTips", thresholdMetCheckpoints.size.toString)

      Some(tipSOE -> finalFacilitators)
    } else None
  }


  def accept(soeCache: SignedObservationEdgeCacheData)(implicit dao: DAO): Unit = this.synchronized {

    if (dao.soeService.lruCache.contains(soeCache.signedObservationEdge.map {
      _.baseHash
    }.getOrElse(""))) {
      dao.metricsManager ! IncrementMetric("soeHeaderAcceptBlockAlreadyStored")
    } else {

      tryWithMetric(EdgeProcessor.acceptHeader(soeCache), "acceptHeader")



      def reuseTips: Boolean = thresholdMetCheckpoints.size < dao.maxWidth

      soeCache.signedObservationEdge.foreach{ soe =>

        acceptedHeadersSinceSnapshot :+= soe.baseHash
        // TODO: Validation
        if (soeCache.parentSOEBaseHashes.isEmpty) {
          dao.metricsManager ! IncrementMetric("headerMissingParents")
        } else {

          updateTips(soeCache.parentSOEBaseHashes, soe, soeCache.partition)

        }

      }

    }

  }

  def updateTips(parentSOEBaseHashes: Seq[String], soe: SignedObservationEdge, partition: Int)(implicit dao: DAO): Unit = {

    def reuseTips: Boolean = thresholdMetCheckpoints.size < dao.maxWidth

    val keysToRemove = parentSOEBaseHashes.flatMap {
      h =>
        thresholdMetCheckpoints.get(h).flatMap { t =>

          def doRemove(): Option[String] = {
            dao.metricsManager ! IncrementMetric("checkpointTipsRemoved")
            Some(h)
          }

          if (reuseTips) {
            if (t.numUses >= 2) {
              doRemove()
            } else {
              None
            }
          } else {
            doRemove()
          }
        }
    }

    val keysToUpdate = parentSOEBaseHashes.flatMap {
      h =>
        thresholdMetCheckpoints.get(h).flatMap { t =>

          def doUpdate(): Option[(String, TipData)] = {
            dao.metricsManager ! IncrementMetric("checkpointTipsIncremented")
            Some(h -> t.copy(numUses = t.numUses + 1))
          }

          if (reuseTips && t.numUses <= 2) {
            doUpdate()
          } else None
        }
    }.toMap

    thresholdMetCheckpoints = thresholdMetCheckpoints +
      (soe.baseHash -> TipData(soe, 0, partition)) ++
      keysToUpdate --
      keysToRemove

    if (acceptedCBSinceSnapshot.contains(soe.baseHash)) {
      dao.metricsManager ! IncrementMetric("checkpointAcceptedButAlreadyInAcceptedCBSinceSnapshot")
    } else {
      acceptedCBSinceSnapshot = acceptedCBSinceSnapshot :+ soe.baseHash
      dao.metricsManager ! UpdateMetric("acceptedCBSinceSnapshot", acceptedCBSinceSnapshot.size.toString)
    }

  }

  // TODO: Synchronize only on values modified by this, same for other functions
  def accept(checkpointCacheData: CheckpointCacheData)(implicit dao: DAO): Unit = this.synchronized {

    if (dao.checkpointService.lruCache.contains(checkpointCacheData.checkpointBlock.map {
      _.baseHash
    }.getOrElse(""))) {

      dao.metricsManager ! IncrementMetric("checkpointAcceptBlockAlreadyStored")

    } else {

      tryWithMetric(acceptCheckpoint(checkpointCacheData), "acceptCheckpoint")


      checkpointCacheData.checkpointBlock.foreach { checkpointBlock =>

        updateTips(checkpointBlock.parentSOEBaseHashes, checkpointBlock.soe, checkpointCacheData.partition)

      }
    }
  }

}


class AtomicTrie[T] {

  val store: TrieMap[String, AtomicReference[T]] = TrieMap()

  def update(
              key: String,
              updateFunc: T => T,
              empty: => T
            ): T = {

    val ref = store.getOrElseUpdate(key, new AtomicReference[T](empty))
    ref.updateAndGet(t => updateFunc(t))

  }

}


// TODO: Use atomicReference increment pattern instead of synchronized

class StorageService[T](size: Int = 50000) {


  val lruCache: MutableLRUCache[String, T] = {
    import com.twitter.storehaus.cache._
    MutableLRUCache[String, T](size)
  }

  // val actualDatastore = ... .update

  // val mutexStore = TrieMap[String, AtomicUpdater]

  // val mutexKeyCache = mutable.Queue()

  // if mutexKeyCache > size :
  // poll and remove from mutexStore?
  // mutexStore.getOrElseUpdate(hash)
  // Map[Address, AtomicUpdater] // computeIfAbsent getOrElseUpdate
  /*  class AtomicUpdater {
      def update(
                  key: String,
                  updateFunc: T => T,
                  empty: => T
                ): T =
        this.synchronized{
          val data = get(key).map {updateFunc}.getOrElse(empty)
          put(key, data)
          data
        }
    }*/

  def delete(keys: Set[String]) = this.synchronized{
    lruCache.multiRemove(keys)
  }

  def get(key: String): Option[T] = this.synchronized{
    lruCache.get(key)
  }

  def put(key: String, cache: T): Unit = this.synchronized{
    lruCache.+=((key, cache))
  }

  def update(
              key: String,
              updateFunc: T => T,
              empty: => T
            ): T =
    this.synchronized{
      val data = get(key).map {updateFunc}.getOrElse(empty)
      put(key, data)
      data
    }


}




// TODO: Make separate one for acceptedCheckpoints vs nonresolved etc.
class CheckpointService(size: Int = 50000) extends StorageService[CheckpointCacheData](size)
class SOEService(size: Int = 10000) extends StorageService[SignedObservationEdgeCacheData](size)
class TransactionService(size: Int = 50000) extends StorageService[TransactionCacheData](size) {
  private val queue = mutable.Queue[TransactionSerialized]()
  private val maxQueueSize = 20
  override def put(
                    key: String,
                    cache: TransactionCacheData
                  ): Unit = {
    val tx = TransactionSerialized(cache.transaction)
    queue.synchronized {
      if (queue.size == maxQueueSize) {
        queue.dequeue()
      }

      queue.enqueue(tx)
      super.put(key, cache)
    }
  }

  def getLast20TX = queue.reverse
}
class AddressService(size: Int = 50000) extends StorageService[AddressCacheData](size)

trait EdgeDAO {

  var processingConfig = ProcessingConfig()

  var partition: Int = -1

  val checkpointService = new CheckpointService(processingConfig.checkpointLRUMaxSize)
  val soeService = new SOEService()
  val transactionService = new TransactionService(processingConfig.transactionLRUMaxSize)
  val addressService = new AddressService(processingConfig.addressLRUMaxSize)

  val threadSafeTXMemPool = new ThreadSafeTXMemPool()
  val threadSafeTipService = new ThreadSafeTipService()

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

  def readyPeers: Map[Id, PeerData] = peerInfo.filter(_._2.peerMetadata.nodeState == NodeState.Ready)

}
