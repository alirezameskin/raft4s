package raft4s.storage.memory

import cats.implicits._
import cats.Monad
import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import raft4s.storage.{Snapshot, SnapshotStorage}

class MemorySnapshotStorage[F[_]: Monad](ref: Ref[F, Option[Snapshot]]) extends SnapshotStorage[F] {

  override def saveSnapshot(snapshot: Snapshot): F[Unit] =
    ref.set(Some(snapshot))

  override def retrieveSnapshot(): F[Option[Snapshot]] =
    ref.get

  override def getLatestSnapshot(): F[Option[Snapshot]] =
    ref.get
}

object MemorySnapshotStorage {
  def empty[F[_]: Concurrent]: F[MemorySnapshotStorage[F]] =
    for {
      empty <- Ref.of[F, Option[Snapshot]](None)
    } yield new MemorySnapshotStorage[F](empty)
}
