package raft4s.effect.storage.file

import cats.effect.Sync
import cats.{Monad, MonadError}
import raft4s.internal.Logger
import raft4s.storage.PersistedState
import raft4s.storage.serialization.Serializer

import java.nio.file.Path

class FileStateStorage[F[_]: Monad: Logger](path: Path)(implicit ME: MonadError[F, Throwable], S: Serializer[PersistedState])
    extends raft4s.storage.file.FileStateStorage[F](path)
    with raft4s.storage.StateStorage[F] {}

object FileStateStorage {

  def open[F[_]: Sync: Logger](path: Path)(implicit S: Serializer[PersistedState]) = new FileStateStorage[F](path)

}
