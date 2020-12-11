package raft4s.storage.memory

import cats.Monad
import raft4s.storage.{Snapshot, SnapshotStorage}

class MemorySnapshotStorage[F[_]: Monad] extends SnapshotStorage[F] {
  var persistedSnapshot: Option[Snapshot] = None

  override def saveSnapshot(snapshot: Snapshot): F[Unit] =
    Monad[F].pure {
      persistedSnapshot = Some(snapshot)
    }

  override def retrieveSnapshot(): F[Option[Snapshot]] =
    Monad[F].pure(persistedSnapshot)
}

object MemorySnapshotStorage {
  def empty[F[_]: Monad] =
    new MemorySnapshotStorage[F]
}
