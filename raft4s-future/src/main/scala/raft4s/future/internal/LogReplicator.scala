package raft4s.future.internal

import raft4s.Node
import raft4s.future.Log
import raft4s.protocol.AppendEntriesResponse

import scala.concurrent.{ExecutionContext, Future}

private[future] class LogReplicator()(implicit EC: ExecutionContext) extends raft4s.internal.LogReplicator[Future] {
  override def replicatedLogs(peerId: Node, term: Long, nextIndex: Long): Future[AppendEntriesResponse] = ???
}

object LogReplicator {
  def build(nodeId: Node, clientProvider: raft4s.internal.RpcClientProvider[Future], log: raft4s.log.Log[Future])(implicit EC: ExecutionContext): raft4s.internal.LogReplicator[Future] =
    new LogReplicator()
}
