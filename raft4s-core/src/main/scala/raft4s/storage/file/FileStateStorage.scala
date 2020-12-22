package raft4s.storage.file

import cats.{Monad, MonadError}
import raft4s.internal.{ErrorLogging, Logger}
import raft4s.storage.{PersistedState, StateStorage}
import raft4s.storage.serialization.Serializer

import java.nio.file.{Files, Path}
import scala.util.Try

abstract class FileStateStorage[F[_]: Monad: Logger](path: Path)(implicit
  ME: MonadError[F, Throwable],
  serializer: Serializer[PersistedState]
) extends StateStorage[F]
    with ErrorLogging[F] {

  override def persistState(state: PersistedState): F[Unit] =
    errorLogging("Error in Persisting State") {
      Monad[F].pure {
        Files.write(path, serializer.toBytes(state))
        ()
      }
    }

  override def retrieveState(): F[Option[PersistedState]] =
    errorLogging("Error in retrieving state from File") {
      Monad[F].pure {
        Try(Files.readAllBytes(path)).toOption.flatMap(serializer.fromBytes)
      }
    }

}
