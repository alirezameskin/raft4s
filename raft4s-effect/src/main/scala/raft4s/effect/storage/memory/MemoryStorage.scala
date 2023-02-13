package raft4s.effect.storage.memory

import cats.effect.Async
import cats.implicits._
import raft4s.Storage

object MemoryStorage {
  def empty[F[_]: Async]: F[Storage[F]] =
    for {
      snapshotStorage <- MemorySnapshotStorage.empty[F]
      stateStorage    <- MemoryStateStorage.empty[F]
      logStorage      <- MemoryLogStorage.empty[F]
    } yield raft4s.Storage[F](logStorage, stateStorage, snapshotStorage)
}
