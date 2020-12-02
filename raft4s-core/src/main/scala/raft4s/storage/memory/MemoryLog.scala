package raft4s.storage.memory

import cats.Monad
import raft4s.log.Log
import raft4s.protocol.LogEntry

import scala.collection.concurrent.TrieMap

class MemoryLog[F[_]: Monad] extends Log[F] {
  var _commitIndex: Long = 0

  val map = TrieMap[Long, LogEntry]()

  override def length: F[Long] =
    Monad[F].pure(map.size)

  override def get(index: Long): F[LogEntry] =
    Monad[F].pure(map.get(index).orNull)

  override def commitLength: F[Long] =
    Monad[F].pure(_commitIndex)

  override def updateCommitLength(index: Long): F[Unit] =
    Monad[F].pure {
      _commitIndex = index
    }

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

object MemoryLog {
  def empty[F[_]: Monad] = new MemoryLog[F]
}
