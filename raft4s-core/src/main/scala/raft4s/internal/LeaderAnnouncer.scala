package raft4s.internal

import cats.Monad
import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._
import io.odin.Logger

private[raft4s] class LeaderAnnouncer[F[_]: Monad: Concurrent: Logger](
  val announcer: Ref[F, Deferred[F, String]]
) {
  def announce(leaderId: String): F[Unit] =
    for {
      _        <- Logger[F].info(s"A new leader is elected among the members. New Leader is '${leaderId}'.")
      deferred <- announcer.get
      _        <- deferred.complete(leaderId)
    } yield ()

  def reset(): F[Unit] =
    for {
      _           <- Logger[F].debug("Resetting the Announcer.")
      newDeferred <- Deferred[F, String]
      _           <- announcer.set(newDeferred)
    } yield ()

  def listen(): F[String] =
    for {
      deferred <- announcer.get
      leader   <- deferred.get
    } yield leader
}

object LeaderAnnouncer {
  def build[F[_]: Monad: Concurrent: Logger]: F[LeaderAnnouncer[F]] =
    for {
      deferred  <- Deferred[F, String]
      announcer <- Ref.of[F, Deferred[F, String]](deferred)
    } yield new LeaderAnnouncer[F](announcer)
}
