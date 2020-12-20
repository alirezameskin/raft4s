package raft4s

import raft4s.internal.Logger

package object effect {

  def odinLogger[F[_]](odinInstance: io.odin.Logger[F]): Logger[F] =
    new Logger[F] {
      override def trace(msg: => String): F[Unit] = odinInstance.trace(msg)

      override def trace(msg: => String, e: Throwable): F[Unit] = odinInstance.trace(msg, e)

      override def trace(msg: => String, ctx: Map[String, String]): F[Unit] = odinInstance.trace(msg, ctx)

      override def trace(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = odinInstance.trace(msg, ctx, e)

      override def debug(msg: => String): F[Unit] = odinInstance.debug(msg)

      override def debug(msg: => String, e: Throwable): F[Unit] = odinInstance.debug(msg, e)

      override def debug(msg: => String, ctx: Map[String, String]): F[Unit] = odinInstance.debug(msg, ctx)

      override def debug(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = odinInstance.debug(msg, ctx, e)

      override def info(msg: => String): F[Unit] = odinInstance.info(msg)

      override def info(msg: => String, e: Throwable): F[Unit] = odinInstance.info(msg, e)

      override def info(msg: => String, ctx: Map[String, String]): F[Unit] = odinInstance.info(msg, ctx)

      override def info(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = odinInstance.info(msg, ctx, e)

      override def warn(msg: => String): F[Unit] = odinInstance.warn(msg)

      override def warn(msg: => String, e: Throwable): F[Unit] = odinInstance.warn(msg, e)

      override def warn(msg: => String, ctx: Map[String, String]): F[Unit] = odinInstance.warn(msg, ctx)

      override def warn(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = odinInstance.warn(msg, ctx, e)

      override def error(msg: => String): F[Unit] = odinInstance.error(msg)

      override def error(msg: => String, e: Throwable): F[Unit] = odinInstance.error(msg, e)

      override def error(msg: => String, ctx: Map[String, String]): F[Unit] = odinInstance.error(msg, ctx)

      override def error(msg: => String, ctx: Map[String, String], e: Throwable): F[Unit] = odinInstance.error(msg, ctx, e)
    }
}
