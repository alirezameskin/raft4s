package raft4s.effect.internal.impl

import cats.Monad
import cats.effect.Concurrent
import cats.effect.{Deferred, Ref}
import cats.implicits._
import raft4s.Node
import raft4s.internal.{LeaderAnnouncer, Logger}

private[effect] class LeaderAnnouncerImpl[F[_]: Monad: Concurrent: Logger](val deferredRef: Ref[F, Deferred[F, Node]])
    extends LeaderAnnouncer[F] {

  def announce(leader: Node): F[Unit] =
    for {
      _        <- Logger[F].info(s"A new leader is elected among the members. New Leader is '${leader}'.")
      deferred <- deferredRef.get
      _        <- deferred.complete(leader)
    } yield ()

  def reset(): F[Unit] =
    for {
      _           <- Logger[F].debug("Resetting the Announcer.")
      newDeferred <- Deferred[F, Node]
      _           <- deferredRef.set(newDeferred)
    } yield ()

  def listen(): F[Node] =
    for {
      deferred <- deferredRef.get
      leader   <- deferred.get
    } yield leader
}

object LeaderAnnouncerImpl {
  def build[F[_]: Monad: Concurrent: Logger]: F[LeaderAnnouncerImpl[F]] =
    for {
      deferred  <- Deferred[F, Node]
      announcer <- Ref.of[F, Deferred[F, Node]](deferred)
    } yield new LeaderAnnouncerImpl[F](announcer)
}
