package raft4s.storage.rocksdb

import cats.effect.{Resource, Sync}
import cats.implicits._
import io.odin.Logger
import org.rocksdb.Options
import org.{rocksdb => jrocks}
import raft4s.protocol.LogEntry
import raft4s.storage.LogStorage
import raft4s.storage.rocksdb.serializer.{LongSerializer, ObjectSerializer}

import java.nio.file.Path
import scala.util.Try

class RocksDBLogStorage[F[_]: Sync: Logger](db: jrocks.RocksDB) extends LogStorage[F] {

  override def length: F[Long] =
    Sync[F].delay {
      val iterator = db.newIterator()
      iterator.seekToLast()

      if (iterator.isValid) {
        val bytes    = iterator.value()
        val logEntry = ObjectSerializer.decode[LogEntry](bytes)
        logEntry.index + 1
      } else {
        0
      }
    }

  override def get(index: Long): F[LogEntry] =
    Sync[F].delay {
      val bytes = db.get(LongSerializer.toBytes(index))
      Option(bytes).map(ObjectSerializer.decode[LogEntry]).orNull
    }

  override def put(index: Long, logEntry: LogEntry): F[LogEntry] =
    Sync[F].delay {
      val bytes = ObjectSerializer.encode(logEntry)
      val key   = LongSerializer.toBytes(index)

      db.put(key, bytes)

      logEntry
    }

  override def delete(index: Long): F[Unit] =
    Sync[F].delay {
      db.delete(LongSerializer.toBytes(index))
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
