package raft4s.storage

import cats.Applicative

class MemorySnapshotStorage[F[_]: Applicative](var stored: Option[Snapshot]) extends SnapshotStorage[F] {

  override def saveSnapshot(snapshot: Snapshot): F[Unit] =
    Applicative[F].pure {
      stored = Some(snapshot)
    }

  override def retrieveSnapshot(): F[Option[Snapshot]] =
    Applicative[F].pure(stored)

  override def getLatestSnapshot(): F[Option[Snapshot]] =
    Applicative[F].pure(stored)
}
