package raft4s.storage.file

import cats.implicits._
import cats.effect.Sync
import cats.effect.concurrent.Ref
import io.odin.Logger
import raft4s.internal.ErrorLogging
import raft4s.protocol.{ClusterConfiguration, JointClusterConfiguration, NewClusterConfiguration}
import raft4s.storage.{Snapshot, SnapshotStorage}
import raft4s.Node

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
      config     <- Try(Files.readAllLines(path.resolve("snapshot_config"))).map(_.asScala.toList).map(decodeConfig)
    } yield Snapshot(lastIndex, bytebuffer, config)

  private def tryStoreInFileSystem(snapshot: Snapshot): Try[Path] = Try {
    Files.write(path.resolve("snapshot_config"), encodeConfig(snapshot.config).asJava, StandardCharsets.UTF_8)
    Files.write(path.resolve("snapshot_state"), List(snapshot.lastIndex.toString).asJava, StandardCharsets.UTF_8)
    Files.write(path.resolve("snapshot"), snapshot.bytes.array())
  }

  private def encodeConfig(config: ClusterConfiguration): List[String] =
    config match {
      case JointClusterConfiguration(oldMembers, newMembers) =>
        List(oldMembers.map(_.id).mkString(","), newMembers.map(_.id).mkString(","))
      case NewClusterConfiguration(members) =>
        List(members.map(_.id).mkString(","))
    }

  private def decodeConfig(lines: List[String]): ClusterConfiguration =
    lines match {
      case first :: Nil =>
        NewClusterConfiguration(first.split(",").map(Node.fromString).filter(_.isDefined).map(_.get).toSet)

      case first :: second :: Nil =>
        JointClusterConfiguration(
          first.split(",").map(Node.fromString).filter(_.isDefined).map(_.get).toSet,
          second.split(",").map(Node.fromString).filter(_.isDefined).map(_.get).toSet
        )

      case _ =>
        NewClusterConfiguration(Set.empty)
    }
}

object FileSnapshotStorage {
  def open[F[_]: Sync: Logger](path: Path): F[FileSnapshotStorage[F]] =
    for {
      snapshot <- Ref.of[F, Option[Snapshot]](None)
    } yield new FileSnapshotStorage[F](path, snapshot)
}
