package raft4s.helpers

import cats.MonadError
import io.odin.Logger

trait ErrorLogging[F[_]] {

  def errorLogging[A](message: String)(fa: F[A])(implicit L: Logger[F], ME: MonadError[F, Throwable]): F[A] =
    ME.attemptTap(fa) {
      case Left(error) => L.warn(s"Error in (${message}):  ${error}")
      case Right(_)    => ME.pure(())
    }
}
