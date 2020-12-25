package raft4s.effect.internal.impl

import cats.Monad
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import raft4s.Node
import raft4s.internal.MembershipManager
import raft4s.protocol.{ClusterConfiguration, NewClusterConfiguration}

import scala.collection.immutable.Set

private[effect] class MembershipManagerImpl[F[_]: Monad](configurationRef: Ref[F, ClusterConfiguration])
    extends MembershipManager[F] {

  def members: F[Set[Node]] =
    configurationRef.get.map(_.members)

  def setClusterConfiguration(newConfig: ClusterConfiguration): F[Unit] =
    configurationRef.set(newConfig)

  def getClusterConfiguration: F[ClusterConfiguration] =
    configurationRef.get

}

object MembershipManagerImpl {
  def build[F[_]: Monad: Sync](members: Set[Node]): F[MembershipManagerImpl[F]] =
    for {
      config <- Ref.of[F, ClusterConfiguration](NewClusterConfiguration(members))
    } yield new MembershipManagerImpl[F](config)
}
