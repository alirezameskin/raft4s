package raft4s.storage.rocksdb

import cats.effect.{Resource, Sync}
import cats.implicits._
import io.odin.Logger
import org.rocksdb._
import org.{rocksdb => jrocks}
import raft4s.storage.Storage

import scala.jdk.CollectionConverters._
import scala.util.Try

object RocksDBStorage {
  val LOGS_COLUMN_FAMILY  = "logs"
  val STATE_COLUMN_FAMILY = "state"
  val DEFAULT_FAMILY      = "default"

  def open[F[_]: Sync: Logger](path: String): Resource[F, Storage[F]] = {
    val acquire = for {
      _      <- Try(jrocks.RocksDB.loadLibrary()).liftTo[F]
      _      <- createRequiredColumnFamilies(path).liftTo[F]
      result <- openDbWithHandles(path).liftTo[F]
    } yield result

    for {
      _         <- Resource.liftF(Try(jrocks.RocksDB.loadLibrary()).liftTo[F])
      resources <- Resource.make(acquire)(r => Sync[F].delay(r._1.close()))

      (db, logsHandle, stateHandle) = resources
      logStorage                    = new RocksDBLogStorage[F](db, logsHandle)
      stateStorage                  = new RocksDBStateStorage[F](db, stateHandle)
    } yield Storage[F](logStorage, stateStorage)
  }

  private def createRequiredColumnFamilies(path: String): Try[Unit] = Try {
    val options   = new jrocks.DBOptions().setCreateIfMissing(true)
    val available = jrocks.RocksDB.listColumnFamilies(new Options(), path).asScala.map(bs => new String(bs))

    val families = if (available.contains(DEFAULT_FAMILY)) available else available.appended(DEFAULT_FAMILY)
    val tmpList  = scala.collection.mutable.ListBuffer.empty[ColumnFamilyHandle].asJava
    val tmpdb = jrocks.RocksDB.open(
      options,
      path,
      families.map(s => new ColumnFamilyDescriptor(s.getBytes, new ColumnFamilyOptions())).toList.asJava,
      tmpList
    )

    if (!available.contains(LOGS_COLUMN_FAMILY)) {
      tmpdb.createColumnFamily(new ColumnFamilyDescriptor(LOGS_COLUMN_FAMILY.getBytes, new ColumnFamilyOptions()))
    }

    if (!available.contains(STATE_COLUMN_FAMILY)) {
      tmpdb.createColumnFamily(new ColumnFamilyDescriptor(STATE_COLUMN_FAMILY.getBytes, new ColumnFamilyOptions()))
    }

    tmpdb.close()
  }

  private def openDbWithHandles(path: String): Try[(RocksDB, ColumnFamilyHandle, ColumnFamilyHandle)] = Try {

    val options = new jrocks.DBOptions().setCreateIfMissing(true)

    val descriptors = List(DEFAULT_FAMILY.getBytes, LOGS_COLUMN_FAMILY.getBytes, STATE_COLUMN_FAMILY.getBytes)
      .map(name => new jrocks.ColumnFamilyDescriptor(name, new ColumnFamilyOptions()))
      .asJava

    val list    = scala.collection.mutable.ListBuffer.empty[ColumnFamilyHandle].asJava
    val db      = jrocks.RocksDB.open(options, path, descriptors, list)
    val handles = list.asScala.map(h => (new String(h.getName), h)).toMap

    val logsHandler  = handles(LOGS_COLUMN_FAMILY)
    val stateHandler = handles(STATE_COLUMN_FAMILY)

    (db, logsHandler, stateHandler)
  }
}
