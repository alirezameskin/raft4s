package raft4s.storage.file

import cats.effect.Sync
import raft4s.storage.{Snapshot, SnapshotStorage}

import java.nio
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Try

class FileSnapshotStorage[F[_]: Sync](path: Path) extends SnapshotStorage[F] {
  override def saveSnapshot(snapshot: Snapshot): F[Unit] =
    Sync[F].delay {
      val content = List(snapshot.lastIndex.toString, snapshot.lastTerm.toString)
      Files.write(path.resolve("snapshot_state"), content.asJava, StandardCharsets.UTF_8)
      Files.write(path.resolve("snapshot"), snapshot.bytes.array())
    }

  override def retrieveSnapshot(): F[Option[Snapshot]] =
    Sync[F].delay {
      val state = for {
        lines      <- Try(Files.readAllLines(path.resolve("snapshot_state"), StandardCharsets.UTF_8).asScala)
        lastIndex  <- Try(lines.head.toLong)
        lastTerm   <- Try(lines.tail.head.toLong)
        bytebuffer <- Try(Files.readAllBytes(path.resolve("snapshot"))).map(nio.ByteBuffer.wrap)
      } yield Snapshot(lastIndex, lastTerm, bytebuffer)

      state.toOption
    }
}

object FileSnapshotStorage {
  def open[F[_]: Sync](path: Path) = new FileSnapshotStorage[F](path)
}
