package raft4s.storage.file

import cats.implicits._
import cats.effect.Sync
import cats.effect.concurrent.Ref
import io.odin.Logger
import raft4s.helpers.ErrorLogging
import raft4s.storage.{Snapshot, SnapshotStorage}

import java.nio
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Try

class FileSnapshotStorage[F[_]: Sync: Logger](path: Path, latestSnapshot: Ref[F, Option[Snapshot]])
    extends SnapshotStorage[F]
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

  private def tryLoadFromFileSystem(): Try[Snapshot] =
    for {
      lines      <- Try(Files.readAllLines(path.resolve("snapshot_state"), StandardCharsets.UTF_8).asScala)
      lastIndex  <- Try(lines.head.toLong)
      bytebuffer <- Try(Files.readAllBytes(path.resolve("snapshot"))).map(nio.ByteBuffer.wrap)
    } yield Snapshot(lastIndex, bytebuffer)

  private def tryStoreInFileSystem(snapshot: Snapshot): Try[Path] = Try {
    val content = List(snapshot.lastIndex.toString)
    Files.write(path.resolve("snapshot_state"), content.asJava, StandardCharsets.UTF_8)
    Files.write(path.resolve("snapshot"), snapshot.bytes.array())
  }
}

object FileSnapshotStorage {
  def open[F[_]: Sync: Logger](path: Path): F[FileSnapshotStorage[F]] =
    for {
      snapshot <- Ref.of[F, Option[Snapshot]](None)
    } yield new FileSnapshotStorage[F](path, snapshot)
}
