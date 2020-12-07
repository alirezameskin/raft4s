package raft4s.storage.rocksdb

import cats.effect.Sync
import io.odin.Logger
import org.{rocksdb => jrocks}
import raft4s.log.Log
import raft4s.protocol.LogEntry

class RocksDBLog[F[_]: Sync: Logger](db: jrocks.RocksDB, logHandler: jrocks.ColumnFamilyHandle) extends Log[F] {

  override def length: F[Long] =
    Sync[F].delay {
      val iterator = db.newIterator(logHandler)
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
      val bytes = db.get(logHandler, LongSerializer.toBytes(index))
      Option(bytes).map(ObjectSerializer.decode[LogEntry]).orNull
    }

  override def put(index: Long, logEntry: LogEntry): F[LogEntry] =
    Sync[F].delay {
      val bytes = ObjectSerializer.encode(logEntry)
      val key   = LongSerializer.toBytes(index)

      db.put(logHandler, key, bytes)

      logEntry
    }

  override def delete(index: Long): F[Unit] =
    Sync[F].delay {
      db.delete(logHandler, LongSerializer.toBytes(index))
    }
}
