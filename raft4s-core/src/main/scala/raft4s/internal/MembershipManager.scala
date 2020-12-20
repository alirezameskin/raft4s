package raft4s.internal

import raft4s.Node
import raft4s.protocol.ClusterConfiguration

import scala.collection.immutable.Set

private[raft4s] trait MembershipManager[F[_]] {

  def members: F[Set[Node]]

  def setClusterConfiguration(newConfig: ClusterConfiguration): F[Unit]

  def getClusterConfiguration: F[ClusterConfiguration]
}
