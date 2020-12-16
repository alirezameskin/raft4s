package raft4s.internal

import cats.Monad
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import raft4s.protocol.{ClusterConfiguration, NewClusterConfiguration}

import scala.collection.immutable.Set

private[raft4s] class MembershipManager[F[_]: Monad](clusterConfiguration: Ref[F, ClusterConfiguration]) {

  def members: F[Set[String]] =
    clusterConfiguration.get.map(_.members)

  def changeConfiguration(newConfig: ClusterConfiguration): F[Unit] =
    clusterConfiguration.set(newConfig)

  def getClusterConfiguration: F[ClusterConfiguration] =
    clusterConfiguration.get
}

object MembershipManager {
  def build[F[_]: Monad: Sync](members: Set[String]): F[MembershipManager[F]] =
    for {
      config <- Ref.of[F, ClusterConfiguration](NewClusterConfiguration(members))
    } yield new MembershipManager[F](config)
}
