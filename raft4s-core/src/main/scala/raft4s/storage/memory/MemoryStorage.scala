package raft4s.storage.memory

import cats.Monad
import raft4s.storage.Storage

object MemoryStorage {
  def empty[F[_]: Monad]: Storage[F] =
    Storage[F](MemoryLogStorage.empty[F], MemoryStateStorage.empty[F])
}
