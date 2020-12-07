package raft4s.storage.memory

import cats.Monad
import raft4s.log.Log
import raft4s.node.NodeState
import raft4s.storage.{PersistedState, Storage}

class MemoryStorage[F[_]: Monad](val log: Log[F]) extends Storage[F] {

  var _commitIndex: Long = 0

  override def persistState(state: PersistedState): F[Unit] = Monad[F].unit

  override def retrievePersistedState(): F[Option[PersistedState]] = Monad[F].pure(Some(PersistedState(0, None)))

  override def commitLength: F[Long] =
    Monad[F].pure(_commitIndex)

  override def updateCommitLength(index: Long): F[Unit] =
    Monad[F].pure {
      _commitIndex = index
    }

}

object MemoryStorage {
  def empty[F[_]: Monad] =
    new MemoryStorage[F](MemoryLog.empty[F])
}
