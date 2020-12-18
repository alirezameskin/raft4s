package raft4s.storage.memory

import cats.Monad
import raft4s.protocol.PersistedState
import raft4s.storage.StateStorage

class MemoryStateStorage[F[_]: Monad] extends StateStorage[F] {

  override def persistState(state: PersistedState): F[Unit] =
    Monad[F].unit

  override def retrieveState(): F[Option[PersistedState]] =
    Monad[F].pure(Some(PersistedState(0, None, -1L)))

}

object MemoryStateStorage {
  def empty[F[_]: Monad]: F[MemoryStateStorage[F]] =
    Monad[F].pure(new MemoryStateStorage[F])
}
