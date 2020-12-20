package raft4s.effect.storage.file

import cats.{Monad, MonadError}
import cats.effect.Sync
import raft4s.internal.Logger

import java.nio.file.Path

class FileStateStorage[F[_]: Monad: Logger](path: Path)(implicit ME: MonadError[F, Throwable])
    extends raft4s.storage.file.FileStateStorage[F](path)
    with raft4s.storage.StateStorage[F] {}

object FileStateStorage {

  def open[F[_]: Sync: Logger](path: Path) = new FileStateStorage[F](path)

}
