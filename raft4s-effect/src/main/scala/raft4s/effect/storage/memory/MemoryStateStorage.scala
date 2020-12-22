package raft4s.effect.storage.memory

import cats.Monad
import raft4s.storage.{PersistedState, StateStorage}

class MemoryStateStorage[F[_]: Monad] extends StateStorage[F] {

  override def persistState(state: PersistedState): F[Unit] =
    Monad[F].unit

  override def retrieveState(): F[Option[PersistedState]] =
    Monad[F].pure(Some(PersistedState(0, None, 0L)))

}

object MemoryStateStorage {
  def empty[F[_]: Monad]: F[MemoryStateStorage[F]] =
    Monad[F].pure(new MemoryStateStorage[F])
}
