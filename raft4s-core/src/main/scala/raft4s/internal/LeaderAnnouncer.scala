package raft4s.internal

import cats.Monad
import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._
import io.odin.Logger
import raft4s.Node

private[raft4s] class LeaderAnnouncer[F[_]: Monad: Concurrent: Logger](
  val announcer: Ref[F, Deferred[F, Node]]
) {
  def announce(leader: Node): F[Unit] =
    for {
      _        <- Logger[F].info(s"A new leader is elected among the members. New Leader is '${leader}'.")
      deferred <- announcer.get
      _        <- deferred.complete(leader)
    } yield ()

  def reset(): F[Unit] =
    for {
      _           <- Logger[F].debug("Resetting the Announcer.")
      newDeferred <- Deferred[F, Node]
      _           <- announcer.set(newDeferred)
    } yield ()

  def listen(): F[Node] =
    for {
      deferred <- announcer.get
      leader   <- deferred.get
    } yield leader
}

object LeaderAnnouncer {
  def build[F[_]: Monad: Concurrent: Logger]: F[LeaderAnnouncer[F]] =
    for {
      deferred  <- Deferred[F, Node]
      announcer <- Ref.of[F, Deferred[F, Node]](deferred)
    } yield new LeaderAnnouncer[F](announcer)
}
