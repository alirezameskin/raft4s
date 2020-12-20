package raft4s.effect.storage.rocksdb

import cats.effect.{Resource, Sync}
import cats.implicits._
import org.rocksdb.Options
import org.{rocksdb => jrocks}
import raft4s.internal.{ErrorLogging, Logger}
import raft4s.internal.serializer.{LongSerializer, ObjectSerializer}
import raft4s.protocol.LogEntry
import raft4s.storage.LogStorage

import java.nio.file.Path
import scala.util.Try

class RocksDBLogStorage[F[_]: Sync: Logger](db: jrocks.RocksDB) extends LogStorage[F] with ErrorLogging[F] {

  override def lastIndex: F[Long] =
    errorLogging("Fetching the log length") {
      Sync[F].delay {
        val iterator = db.newIterator()
        iterator.seekToLast()

        if (iterator.isValid) {
          val bytes    = iterator.value()
          val logEntry = ObjectSerializer.decode[LogEntry](bytes)
          logEntry.index
        } else {
          0
        }
      }
    }

  override def get(index: Long): F[LogEntry] =
    errorLogging(s"Fetching a LogEntry at index ${index}") {
      Sync[F].delay {
        val bytes = db.get(LongSerializer.toBytes(index))
        Option(bytes).map(ObjectSerializer.decode[LogEntry]).orNull
      }
    }

  override def put(index: Long, logEntry: LogEntry): F[LogEntry] =
    errorLogging(s"Putting a LogEntry at index ${index}") {
      Sync[F].delay {
        val bytes = ObjectSerializer.encode(logEntry)
        val key   = LongSerializer.toBytes(index)

        db.put(key, bytes)

        logEntry
      }
    }

  override def deleteBefore(index: Long): F[Unit] =
    errorLogging(s"Deleting LogEntries before ${index}") {
      Sync[F].delay {

        val itr = db.newIterator()
        itr.seekToFirst()

        val iterator = new Iterator[Long] {
          override def hasNext: Boolean = itr.isValid
          override def next(): Long = {
            val index = LongSerializer.toLong(itr.key())
            itr.next()

            index
          }
        }

        iterator.takeWhile(_ < index).map(LongSerializer.toBytes).foreach(db.delete)
      }
    }

  override def deleteAfter(index: Long): F[Unit] =
    errorLogging(s"Deleting LogEntries after ${index}") {
      Sync[F].delay {

        val itr = db.newIterator()
        itr.seek(LongSerializer.toBytes(index))

        val iterator = new Iterator[Long] {
          override def hasNext: Boolean = itr.isValid
          override def next(): Long = {
            val index = LongSerializer.toLong(itr.key())
            itr.next()

            index
          }
        }

        iterator.takeWhile(_ > index).map(LongSerializer.toBytes).foreach(db.delete)
      }
    }
}

object RocksDBLogStorage {

  def open[F[_]: Sync: Logger](path: Path): Resource[F, LogStorage[F]] = {
    val options = new Options().setCreateIfMissing(true)

    val acquire = for {
      _  <- Try(jrocks.RocksDB.loadLibrary()).liftTo[F]
      db <- Try(jrocks.RocksDB.open(options, path.toAbsolutePath.toString)).liftTo[F]
    } yield db

    for {
      _  <- Resource.liftF(Try(jrocks.RocksDB.loadLibrary()).liftTo[F])
      db <- Resource.make(acquire)(d => Sync[F].delay(d.close()))

    } yield new RocksDBLogStorage[F](db)
  }
}
