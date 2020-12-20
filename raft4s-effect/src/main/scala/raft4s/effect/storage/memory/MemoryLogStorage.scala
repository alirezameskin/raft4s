package raft4s.effect.storage.memory

import cats.Monad
import raft4s.protocol.LogEntry
import raft4s.storage.LogStorage

import scala.collection.mutable

class MemoryLogStorage[F[_]: Monad] extends LogStorage[F] {

  private val map = mutable.TreeMap.empty[Long, LogEntry]

  override def lastIndex: F[Long] =
    Monad[F].pure(map.size)

  override def get(index: Long): F[LogEntry] =
    Monad[F].pure(map.get(index).orNull)

  override def put(index: Long, logEntry: LogEntry): F[LogEntry] =
    Monad[F].pure {
      map.put(index, logEntry)
      logEntry
    }

  override def deleteBefore(index: Long): F[Unit] =
    Monad[F].pure {
      map.keysIterator.takeWhile(_ < index).foreach(map.remove)
    }

  override def deleteAfter(index: Long): F[Unit] =
    Monad[F].pure {
      map.keysIterator.withFilter(_ > index).foreach(map.remove)
    }
}

object MemoryLogStorage {
  def empty[F[_]: Monad]: F[MemoryLogStorage[F]] =
    Monad[F].pure(new MemoryLogStorage[F])
}
