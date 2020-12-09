package raft4s.storage.memory

import cats.Monad
import raft4s.storage.{PersistedState, StateStorage}

class MemoryStateStorage[F[_]: Monad] extends StateStorage[F] {

  override def persistState(state: PersistedState): F[Unit] = Monad[F].unit

  override def retrieveState(): F[Option[PersistedState]] = Monad[F].pure(Some(PersistedState(0, None)))

}

object MemoryStateStorage {
  def empty[F[_]: Monad] =
    new MemoryStateStorage[F]
}
