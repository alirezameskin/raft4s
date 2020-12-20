package raft4s.internal

trait Logger[F[_]] {

  def trace(msg: => String): F[Unit]

  def trace(msg: => String, e: Throwable): F[Unit]

  def trace(msg: => String, ctx: Map[String, String]): F[Unit]

  def trace(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit]

  def debug(msg: => String): F[Unit]

  def debug(msg: => String, e: Throwable): F[Unit]

  def debug(msg: => String, ctx: Map[String, String]): F[Unit]

  def debug(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit]

  def info(msg: => String): F[Unit]

  def info(msg: => String, e: Throwable): F[Unit]

  def info(msg: => String, ctx: Map[String, String]): F[Unit]

  def info(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit]

  def warn(msg: => String): F[Unit]

  def warn(msg: => String, e: Throwable): F[Unit]

  def warn(msg: => String, ctx: Map[String, String]): F[Unit]

  def warn(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit]

  def error(msg: => String): F[Unit]

  def error(msg: => String, e: Throwable): F[Unit]

  def error(msg: => String, ctx: Map[String, String]): F[Unit]

  def error(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit]
}

object Logger {
  def apply[F[_]](implicit instance: Logger[F]): Logger[F] = instance
}
