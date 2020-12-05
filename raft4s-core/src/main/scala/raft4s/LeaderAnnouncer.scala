package raft4s

import cats.implicits._
import cats.Monad
import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Ref}

class LeaderAnnouncer[F[_]: Monad: Concurrent](
  val announcer: Ref[F, Deferred[F, String]]
) {
  def announce(leaderId: String): F[Unit] =
    for {
      deferred <- announcer.get
      _        <- deferred.complete(leaderId)
    } yield ()

  def reset(): F[Unit] =
    for {
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
  def build[F[_]: Monad: Concurrent]: F[LeaderAnnouncer[F]] =
    for {
      deferred  <- Deferred[F, String]
      announcer <- Ref.of[F, Deferred[F, String]](deferred)
    } yield new LeaderAnnouncer[F](announcer)
}
