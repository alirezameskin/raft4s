package raft4s

import cats.Applicative
import raft4s.internal.Logger

package object future {

  def consoleLogger[F[_]: Applicative](): Logger[F] =
    new Logger[F] {
      override def trace(msg: => String): F[Unit] = Applicative[F].pure(println(msg))

      override def trace(msg: => String, e: Throwable): F[Unit] = Applicative[F].pure(println(msg))

      override def trace(msg: => String, ctx: Map[String, String]): F[Unit] = Applicative[F].pure(println(msg))

      override def trace(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = Applicative[F].pure(println(msg))

      override def debug(msg: => String): F[Unit] = Applicative[F].pure(println(msg))

      override def debug(msg: => String, e: Throwable): F[Unit] = Applicative[F].pure(println(msg))

      override def debug(msg: => String, ctx: Map[String, String]): F[Unit] = Applicative[F].pure(println(msg))

      override def debug(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = Applicative[F].pure(println(msg))

      override def info(msg: => String): F[Unit] = Applicative[F].pure(println(msg))

      override def info(msg: => String, e: Throwable): F[Unit] = Applicative[F].pure(println(msg))

      override def info(msg: => String, ctx: Map[String, String]): F[Unit] = Applicative[F].pure(println(msg))

      override def info(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = Applicative[F].pure(println(msg))

      override def warn(msg: => String): F[Unit] = Applicative[F].pure(println(msg))

      override def warn(msg: => String, e: Throwable): F[Unit] = Applicative[F].pure(println(msg))

      override def warn(msg: => String, ctx: Map[String, String]): F[Unit] = Applicative[F].pure(println(msg))

      override def warn(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = Applicative[F].pure(println(msg))

      override def error(msg: => String): F[Unit] = Applicative[F].pure(println(msg))

      override def error(msg: => String, e: Throwable): F[Unit] = Applicative[F].pure(println(msg))

      override def error(msg: => String, ctx: Map[String, String]): F[Unit] = Applicative[F].pure(println(msg))

      override def error(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = Applicative[F].pure(println(msg))
    }
}
