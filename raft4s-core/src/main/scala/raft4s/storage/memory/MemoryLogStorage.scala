package raft4s.storage.memory

import cats.Monad
import raft4s.protocol.LogEntry
import raft4s.storage.LogStorage

import scala.collection.concurrent.TrieMap

class MemoryLogStorage[F[_]: Monad] extends LogStorage[F] {

  private val map = TrieMap[Long, LogEntry]()

  override def length: F[Long] =
    Monad[F].pure(map.size)

  override def get(index: Long): F[LogEntry] =
    Monad[F].pure(map.get(index).orNull)

  override def put(index: Long, logEntry: LogEntry): F[LogEntry] =
    Monad[F].pure {
      map.put(index, logEntry)
      logEntry
    }

  override def delete(index: Long): F[Unit] =
    Monad[F].pure {
      map.remove(index)
    }
}

object MemoryLogStorage {
  def empty[F[_]: Monad] = new MemoryLogStorage[F]
}
