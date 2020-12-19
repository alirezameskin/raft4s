package raft4s.internal

import cats.implicits._
import cats.MonadError
import io.odin.Logger

private[raft4s] trait ErrorLogging[F[_]] {

  def errorLogging[A](message: String)(fa: F[A])(implicit L: Logger[F], ME: MonadError[F, Throwable]): F[A] =
    ME.attemptTap(fa) {
      case Left(error) =>
        L.warn(s"Error in (${message}):  ${error}")

      case Right(_) => ME.pure(())
    }
}
