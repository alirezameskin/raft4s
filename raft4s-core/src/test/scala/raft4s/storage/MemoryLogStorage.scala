package raft4s.storage

import cats.Monad
import cats.implicits._
import raft4s.LogEntry

import scala.collection.concurrent.TrieMap

class MemoryLogStorage[F[_]: Monad](val map: TrieMap[Long, LogEntry], var last: Long = 0) extends LogStorage[F] {
  override def lastIndex: F[Long] = Monad[F].pure(last)

  override def get(index: Long): F[LogEntry] = Monad[F].pure(map.get(index).orNull)

  override def put(index: Long, logEntry: LogEntry): F[LogEntry] =
    Monad[F].pure {
      map.put(index, logEntry)
      last = Math.max(index, last)
      logEntry
    }

  override def deleteBefore(index: Long): F[Unit] =
    Monad[F].pure(map.keysIterator.filter(_ < index).foreach(map.remove))

  override def deleteAfter(index: Long): F[Unit] =
    Monad[F].pure(map.keysIterator.filter(_ > index).foreach(map.remove))
}
