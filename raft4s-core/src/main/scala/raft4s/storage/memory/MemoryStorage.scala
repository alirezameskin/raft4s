package raft4s.storage.memory

import cats.implicits._
import cats.effect.Concurrent
import raft4s.storage.Storage

object MemoryStorage {
  def empty[F[_]: Concurrent]: F[Storage[F]] =
    for {
      snapshotStorage <- MemorySnapshotStorage.empty[F]
      stateStorage    <- MemoryStateStorage.empty[F]
      logStorage      <- MemoryLogStorage.empty[F]
    } yield Storage[F](logStorage, stateStorage, snapshotStorage)
}
