package org.constellation

import java.io.File

import akka.actor.Actor
import akka.util.Timeout
import com.google.common.cache.CacheBuilder
import constellation.{ParseExt, SerExt}
import org.constellation.LevelDB.RestartDB
import org.constellation.metrics.MetricsActor
import org.constellation.serializer.KryoSerializer
import org.constellation.util.ProductHash
import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._
import scalacache.guava.GuavaCache
import scalacache.modes.sync._
import scalacache._

import scala.tools.nsc.io.{File => SFile}
import scala.util.Try

// https://doc.akka.io/docs/akka/2.5/persistence-query-leveldb.html

object LevelDB {

  case object RestartDB
  case class DBGet(key: String)
  case class DBPut(key: String, obj: AnyRef)
  case class DBDelete(key: String)


}

import org.constellation.LevelDB._

class LevelDBActor(dbId: String)(implicit timeoutI: Timeout) extends Actor {

  import MetricsActor._

  var db: LevelDB = _

  val underlyingGuavaCache = CacheBuilder.newBuilder().maximumSize(1000L).build[String, Entry[Option[AnyRef]]]
  implicit val guavaCache: Cache[Option[AnyRef]] = GuavaCache(underlyingGuavaCache)

  def tmpDirId = new File("tmp", dbId)
  def mkDB = db = new LevelDB(new File(tmpDirId, "db"))
//  def getTmpFile = File.createTempFile("lvldb", "")
//  def mkDB = db = new LevelDB(getTmpFile)

  def restartDB(): Unit = {
    removeAll()
    Try {
      db.destroy()
    }
    mkDB
  }

  restartDB()

  def publishMetric(r: MRecord): Unit = {
    context.system.eventStream.publish(r)
  }

  override def receive: Receive = {
    case RestartDB =>
      restartDB()
    case DBGet(key) =>
      val res = caching(key)(None) {
        db.getBytes(key).map {KryoSerializer.deserialize}
      }
      sender() ! res
      publishMetric(MDBGet(1))
    case DBPut(key, obj) =>
      put(key)(Option(obj))
      val bytes = KryoSerializer.serializeAnyRef(obj)
      db.putBytes(key, bytes)
      publishMetric(MDBPut(1))
    case DBDelete(key) =>
      remove(key)
      if (db.contains(key)) {
        sender() ! db.delete(key).isSuccess
        publishMetric(MDBDelete(1))
      } else sender() ! true
  }

}
// Only need to implement kryo get / put

class LevelDB(val file: File) {
  val options = new Options()
  options.createIfMissing(true)
  Try{file.mkdirs}
  val db: DB = factory.open(file, options)

  // Either
  def get(s: String) = Option(asString(db.get(bytes(s))))
  def getBytes(s: String): Option[Array[Byte]] = Option(db.get(bytes(s))).filter(_.nonEmpty)
  def put(k: String, v: Array[Byte]) = Try {db.put(bytes(k), v)}
  def contains(s: String): Boolean = getBytes(s).nonEmpty
  def contains[T <: ProductHash](t: T): Boolean = getBytes(t.hash).nonEmpty
  def putStr(k: String, v: String) = Try {db.put(bytes(k), bytes(v))}
  def putBytes(k: String, v: Array[Byte]) = Try {db.put(bytes(k), v)}
  def put(k: String, v: String) = Try {db.put(bytes(k), bytes(v))}
  def putHash[T <: ProductHash, Q <: ProductHash](t: T, q: Q): Try[Unit] = put(t.hash, q.hash)

  // JSON
  def getAsJson[T](s: String)(implicit m: Manifest[T]): Option[T] = get(s).map{_.x[T]}
  def getHashAsJson[T](s: ProductHash)(implicit m: Manifest[T]): Option[T] = get(s.hash).map{_.x[T]}
  def getRaw(s: String): String = asString(db.get(bytes(s)))
  def getSafe(s: String): Try[String] = Try{asString(db.get(bytes(s)))}
  def putJson[T <: ProductHash, Q <: AnyRef](t: T, q: Q): Try[Unit] = put(t.hash, q.json)
  def putJson(k: String, t: AnyRef): Try[Unit] = put(k, t.json)
  def putJson[T <: ProductHash](t: T): Try[Unit] = put(t.hash, t.json)

/*
  // Kryo
  def getAs[T](s: String)(implicit m: Manifest[T]): Option[T] = getBytes(s).map{_.kryoExtract[T]}
  def getHashAs[T](s: ProductHash)(implicit m: Manifest[T]): Option[T] = getBytes(s.hash).map{_.kryoExtract[T]}
  def put[T <: ProductHash, Q <: AnyRef](t: T, q: Q): Try[Unit] = putBytes(t.hash, q.kryoWrite)
  def put(k: String, t: AnyRef): Try[Unit] = putBytes(k, t.kryoWrite)
  def put[T <: ProductHash](t: T): Try[Unit] = putBytes(t.hash, t.kryoWrite)
*/


  // Util

  def delete(k: String) = Try{db.delete(bytes(k))}
  def close(): Unit = db.close()
  def destroy(): Unit = {
    close()
    SFile(file).deleteRecursively()
  }

}