package raft4s.future.internal

import raft4s.Node
import raft4s.protocol.{AppendEntries, AppendEntriesResponse, Command, LogEntry, VoteRequest, VoteResponse}
import raft4s.rpc.RpcClient
import raft4s.storage.Snapshot

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

private[future] class RpcClientProvider(clients: Map[Node, RpcClient[Future]])(implicit EC: ExecutionContext) extends raft4s.internal.RpcClientProvider[Future] {

  private val clientsRef = new AtomicReference[Map[Node, RpcClient[Future]]](clients)

  override def send(serverId: Node, voteRequest: VoteRequest): Future[VoteResponse] = ???

  override def send(serverId: Node, appendEntries: AppendEntries): Future[AppendEntriesResponse] = ???

  override def send(serverId: Node, snapshot: Snapshot, lastEntry: LogEntry): Future[AppendEntriesResponse] = ???

  override def send[T](serverId: Node, command: Command[T]): Future[T] = ???

  override def join(serverId: Node, newNode: Node): Future[Boolean] = ???

  override def closeConnections(): Future[Unit] =
    Future.sequence(clientsRef.get().map(_._2.close())).map(_ => ())
}

object RpcClientProvider {
  def build(implicit EC: ExecutionContext): raft4s.internal.RpcClientProvider[Future] =
    new RpcClientProvider(Map.empty)
}
