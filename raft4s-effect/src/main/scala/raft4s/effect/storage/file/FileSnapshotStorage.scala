package raft4s.effect.storage.file

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import raft4s.internal.{ErrorLogging, Logger}
import raft4s.storage.Snapshot

import java.nio.file.Path

class FileSnapshotStorage[F[_]: Sync: Logger](path: Path, latestSnapshot: Ref[F, Option[Snapshot]])
    extends raft4s.storage.file.FileSnapshotStorage[F](path)
    with ErrorLogging[F] {

  override def getLatestSnapshot(): F[Option[Snapshot]] =
    latestSnapshot.get

  override def saveSnapshot(snapshot: Snapshot): F[Unit] =
    errorLogging(s"Saving Snapshot in file system ${path}") {
      for {
        _ <- Sync[F].delay(tryStoreInFileSystem(snapshot))
        _ <- latestSnapshot.set(Some(snapshot))
      } yield ()
    }

  override def retrieveSnapshot(): F[Option[Snapshot]] =
    errorLogging(s"Retrieving Snapshot from file system ${path}") {
      for {
        snapshot <- Sync[F].delay(tryLoadFromFileSystem().toOption)
        _        <- latestSnapshot.set(snapshot)
      } yield snapshot
    }

}

object FileSnapshotStorage {
  def open[F[_]: Sync: Logger](path: Path): F[FileSnapshotStorage[F]] =
    for {
      snapshot <- Ref.of[F, Option[Snapshot]](None)
    } yield new FileSnapshotStorage[F](path, snapshot)
}
