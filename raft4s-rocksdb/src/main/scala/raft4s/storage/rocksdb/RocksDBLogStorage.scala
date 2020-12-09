package raft4s.storage.rocksdb

import cats.effect.Sync
import io.odin.Logger
import org.{rocksdb => jrocks}
import raft4s.protocol.LogEntry
import raft4s.storage.LogStorage
import raft4s.storage.rocksdb.serializer.{LongSerializer, ObjectSerializer}

class RocksDBLogStorage[F[_]: Sync: Logger](db: jrocks.RocksDB, handle: jrocks.ColumnFamilyHandle) extends LogStorage[F] {

  override def length: F[Long] =
    Sync[F].delay {
      val iterator = db.newIterator(handle)
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
      val bytes = db.get(handle, LongSerializer.toBytes(index))
      Option(bytes).map(ObjectSerializer.decode[LogEntry]).orNull
    }

  override def put(index: Long, logEntry: LogEntry): F[LogEntry] =
    Sync[F].delay {
      val bytes = ObjectSerializer.encode(logEntry)
      val key   = LongSerializer.toBytes(index)

      db.put(handle, key, bytes)

      logEntry
    }

  override def delete(index: Long): F[Unit] =
    Sync[F].delay {
      db.delete(handle, LongSerializer.toBytes(index))
    }
}
