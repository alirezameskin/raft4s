package raft4s.future.storage.rocksdb

import cats.Monad
import cats.implicits._
import org.rocksdb.Options
import org.{rocksdb => jrocks}
import raft4s.LogEntry
import raft4s.internal.{ErrorLogging, Logger}
import raft4s.storage.LogStorage
import raft4s.storage.serialization.Serializer

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}

class RocksDBLogStorage(db: jrocks.RocksDB)(implicit
  LS: Serializer[Long],
  ES: Serializer[LogEntry],
  EC: ExecutionContext,
  L: Logger[Future]
) extends LogStorage[Future]
    with ErrorLogging[Future] {

  override def lastIndex: Future[Long] =
    errorLogging("Fetching the log length") {
      Monad[Future].pure {
        tryLastIndex.flatMap(Serializer[LogEntry].fromBytes).map(_.index).getOrElse(0)
      }
    }

  private def tryLastIndex: Option[Array[Byte]] = {
    val iterator = db.newIterator()
    iterator.seekToLast()

    if (iterator.isValid) {
      Option(iterator.value())
    } else {
      None
    }
  }

  override def get(index: Long): Future[LogEntry] =
    errorLogging(s"Fetching a LogEntry at index ${index}") {
      Monad[Future].pure {
        val bytes = db.get(Serializer[Long].toBytes(index))
        Option(bytes).flatMap(Serializer[LogEntry].fromBytes).orNull
      }
    }

  override def put(index: Long, logEntry: LogEntry): Future[LogEntry] =
    errorLogging(s"Putting a LogEntry at index ${index}") {
      Monad[Future].pure {
        val bytes = Serializer[LogEntry].toBytes(logEntry)
        val key   = Serializer[Long].toBytes(index)

        db.put(key, bytes)

        logEntry
      }
    }

  override def deleteBefore(index: Long): Future[Unit] =
    errorLogging(s"Deleting LogEntries before ${index}") {
      Monad[Future].pure {

        val itr = db.newIterator()
        itr.seekToFirst()

        val iterator = new Iterator[Long] {
          override def hasNext: Boolean = itr.isValid
          override def next(): Long = {
            val index = Serializer[Long].fromBytes(itr.key())
            itr.next()

            index.get
          }
        }

        iterator.takeWhile(_ < index).map(Serializer[Long].toBytes).foreach(db.delete)
      }
    }

  override def deleteAfter(index: Long): Future[Unit] =
    errorLogging(s"Deleting LogEntries after ${index}") {
      Monad[Future].pure {

        val itr = db.newIterator()
        itr.seek(Serializer[Long].toBytes(index))

        val iterator = new Iterator[Long] {
          override def hasNext: Boolean = itr.isValid
          override def next(): Long = {
            val index = Serializer[Long].fromBytes(itr.key())
            itr.next()

            index.get
          }
        }

        iterator.takeWhile(_ > index).map(Serializer[Long].toBytes).foreach(db.delete)
      }
    }
}

object RocksDBLogStorage {

  def open(
    path: Path
  )(implicit L: Serializer[Long], E: Serializer[LogEntry], EC: ExecutionContext, LG: Logger[Future]): LogStorage[Future] = {
    val options = new Options().setCreateIfMissing(true)

    jrocks.RocksDB.loadLibrary()
    val db = jrocks.RocksDB.open(options, path.toAbsolutePath.toString)

    new RocksDBLogStorage(db)
  }

}
