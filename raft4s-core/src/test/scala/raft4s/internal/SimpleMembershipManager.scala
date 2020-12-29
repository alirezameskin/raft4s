package raft4s.internal

import cats.Applicative
import raft4s.Node
import raft4s.protocol.ClusterConfiguration

class SimpleMembershipManager[F[_]: Applicative](var config: ClusterConfiguration) extends MembershipManager[F] {

  override def members: F[Set[Node]] =
    Applicative[F].pure {
      config.members
    }

  override def setClusterConfiguration(newConfig: ClusterConfiguration): F[Unit] =
    Applicative[F].pure {
      config = newConfig
    }

  override def getClusterConfiguration: F[ClusterConfiguration] =
    Applicative[F].pure {
      config
    }
}
