package raft4s.internal

import cats.Monad
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import raft4s.protocol.{ClusterConfiguration, NewClusterConfiguration, Node}

import scala.collection.immutable.Set

private[raft4s] class MembershipManager[F[_]: Monad](clusterConfiguration: Ref[F, ClusterConfiguration]) {

  def members: F[Set[Node]] =
    clusterConfiguration.get.map(_.members)

  def setClusterConfiguration(newConfig: ClusterConfiguration): F[Unit] =
    clusterConfiguration.set(newConfig)

  def getClusterConfiguration: F[ClusterConfiguration] =
    clusterConfiguration.get
}

object MembershipManager {
  def build[F[_]: Monad: Sync](members: Set[Node]): F[MembershipManager[F]] =
    for {
      config <- Ref.of[F, ClusterConfiguration](NewClusterConfiguration(members))
    } yield new MembershipManager[F](config)
}
