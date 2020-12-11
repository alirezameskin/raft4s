package raft4s.storage.file

import cats.effect.Sync
import raft4s.storage.StateStorage
import raft4s.storage.internal.PersistedState

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Try

class FileStateStorage[F[_]: Sync](path: Path) extends StateStorage[F] {

  override def persistState(state: PersistedState): F[Unit] =
    Sync[F].delay {
      val content = List(state.term.toString, state.votedFor.getOrElse(""))

      Files.write(path, content.asJava, StandardCharsets.UTF_8)
    }

  override def retrieveState(): F[Option[PersistedState]] =
    Sync[F].delay {
      val state = for {
        lines <- Try(Files.readAllLines(path, StandardCharsets.UTF_8).asScala)
        term  <- Try(lines.head.toLong)
        voted <- Try(lines.tail.headOption)
      } yield PersistedState(term, voted)

      state.toOption
    }
}

object FileStateStorage {
  def open[F[_]: Sync](path: Path) = new FileStateStorage[F](path)
}
