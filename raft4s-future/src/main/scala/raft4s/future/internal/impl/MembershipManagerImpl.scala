package raft4s.future.internal.impl

import raft4s.Node
import raft4s.internal.MembershipManager
import raft4s.protocol.{ClusterConfiguration, NewClusterConfiguration}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

private[future] class MembershipManagerImpl(config: ClusterConfiguration)(implicit EC: ExecutionContext)
    extends MembershipManager[Future] {

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

object MembershipManagerImpl {
  def build(members: Set[Node])(implicit EC: ExecutionContext): MembershipManager[Future] =
    new MembershipManagerImpl(NewClusterConfiguration(members))
}
