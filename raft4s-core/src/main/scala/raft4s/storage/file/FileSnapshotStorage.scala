package raft4s.storage.file

import raft4s.Node
import raft4s.internal.Logger
import raft4s.protocol.{ClusterConfiguration, JointClusterConfiguration, NewClusterConfiguration}
import raft4s.storage.{Snapshot, SnapshotStorage}

import java.nio
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Try

abstract class FileSnapshotStorage[F[_]: Logger](path: Path) extends SnapshotStorage[F] {

  protected def tryLoadFromFileSystem(): Try[Snapshot] =
    for {
      lines      <- Try(Files.readAllLines(path.resolve("snapshot_state"), StandardCharsets.UTF_8).asScala)
      lastIndex  <- Try(lines.head.toLong)
      bytebuffer <- Try(Files.readAllBytes(path.resolve("snapshot"))).map(nio.ByteBuffer.wrap)
      config     <- Try(Files.readAllLines(path.resolve("snapshot_config"))).map(_.asScala.toList).map(decodeConfig)
    } yield Snapshot(lastIndex, bytebuffer, config)

  protected def tryStoreInFileSystem(snapshot: Snapshot): Try[Path] = Try {
    Files.write(path.resolve("snapshot_config"), encodeConfig(snapshot.config).asJava, StandardCharsets.UTF_8)
    Files.write(path.resolve("snapshot_state"), List(snapshot.lastIndex.toString).asJava, StandardCharsets.UTF_8)
    Files.write(path.resolve("snapshot"), snapshot.bytes.array())
  }

  protected def encodeConfig(config: ClusterConfiguration): List[String] =
    config match {
      case JointClusterConfiguration(oldMembers, newMembers) =>
        List(oldMembers.map(_.id).mkString(","), newMembers.map(_.id).mkString(","))
      case NewClusterConfiguration(members) =>
        List(members.map(_.id).mkString(","))
    }

  protected def decodeConfig(lines: List[String]): ClusterConfiguration =
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
