package raft4s.effect.internal

import cats.Monad
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import raft4s.Node
import raft4s.protocol.{ClusterConfiguration, NewClusterConfiguration}

import scala.collection.immutable.Set

private[effect] class MembershipManager[F[_]: Monad](clusterConfiguration: Ref[F, ClusterConfiguration])
    extends raft4s.internal.MembershipManager[F] {

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
