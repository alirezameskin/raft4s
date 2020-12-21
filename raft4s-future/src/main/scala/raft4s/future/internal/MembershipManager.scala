package raft4s.future.internal

import raft4s.Node
import raft4s.protocol.{ClusterConfiguration, NewClusterConfiguration}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

private[future] class MembershipManager(config: ClusterConfiguration)(implicit EC: ExecutionContext)
    extends raft4s.internal.MembershipManager[Future] {
  private val configRef = new AtomicReference[ClusterConfiguration](config)

  override def members: Future[Set[Node]] =
    Future {
      configRef.get().members
    }

  override def setClusterConfiguration(newConfig: ClusterConfiguration): Future[Unit] =
    Future {
      configRef.set(newConfig)
    }

  override def getClusterConfiguration: Future[ClusterConfiguration] =
    Future {
      configRef.get()
    }
}

object MembershipManager {
  def build(members: Set[Node])(implicit EC: ExecutionContext): raft4s.internal.MembershipManager[Future] =
    new MembershipManager(NewClusterConfiguration(members))
}
