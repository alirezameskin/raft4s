package raft4s.internal

import cats.Applicative

class NullLogger[F[_]: Applicative] extends Logger[F] {
  override def trace(msg: => String): F[Unit] = Applicative[F].unit

  override def trace(msg: => String, e: Throwable): F[Unit] = Applicative[F].unit

  override def trace(msg: => String, ctx: Map[String, String]): F[Unit] = Applicative[F].unit

  override def trace(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = Applicative[F].unit

  override def debug(msg: => String): F[Unit] = Applicative[F].unit

  override def debug(msg: => String, e: Throwable): F[Unit] = Applicative[F].unit

  override def debug(msg: => String, ctx: Map[String, String]): F[Unit] = Applicative[F].unit

  override def debug(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = Applicative[F].unit

  override def info(msg: => String): F[Unit] = Applicative[F].unit

  override def info(msg: => String, e: Throwable): F[Unit] = Applicative[F].unit

  override def info(msg: => String, ctx: Map[String, String]): F[Unit] = Applicative[F].unit

  override def info(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = Applicative[F].unit

  override def warn(msg: => String): F[Unit] = Applicative[F].unit

  override def warn(msg: => String, e: Throwable): F[Unit] = Applicative[F].unit

  override def warn(msg: => String, ctx: Map[String, String]): F[Unit] = Applicative[F].unit

  override def warn(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = Applicative[F].unit

  override def error(msg: => String): F[Unit] = Applicative[F].unit

  override def error(msg: => String, e: Throwable): F[Unit] = Applicative[F].unit

  override def error(msg: => String, ctx: Map[String, String]): F[Unit] = Applicative[F].unit

  override def error(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = Applicative[F].unit
}
