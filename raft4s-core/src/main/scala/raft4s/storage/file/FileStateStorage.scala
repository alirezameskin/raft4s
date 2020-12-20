package raft4s.storage.file

import cats.{Monad, MonadError}
import raft4s.Node
import raft4s.internal.{ErrorLogging, Logger}
import raft4s.protocol.PersistedState
import raft4s.storage.StateStorage

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Try

abstract class FileStateStorage[F[_]: Monad: Logger](path: Path)(implicit ME: MonadError[F, Throwable])
    extends StateStorage[F]
    with ErrorLogging[F] {

  override def persistState(state: PersistedState): F[Unit] =
    errorLogging("Error in Persisting State") {
      Monad[F].pure {
        val content = List(state.term.toString, state.votedFor.map(_.id).getOrElse(""), state.appliedIndex.toString)

        Files.write(path, content.asJava, StandardCharsets.UTF_8)
        ()
      }
    }

  override def retrieveState(): F[Option[PersistedState]] =
    errorLogging("Error in retrieving state from File") {
      Monad[F].pure {
        val state = for {
          lines        <- Try(Files.readAllLines(path, StandardCharsets.UTF_8).asScala)
          term         <- Try(lines.head.toLong)
          voted        <- Try(lines.tail.headOption)
          appliedIndex <- Try(lines.tail.tail.headOption.map(_.toLong).getOrElse(0L))
        } yield PersistedState(term, voted.flatMap(Node.fromString), appliedIndex)

        state.toOption
      }
    }

}
