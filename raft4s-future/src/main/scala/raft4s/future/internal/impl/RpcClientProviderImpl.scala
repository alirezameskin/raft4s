package raft4s.future.internal.impl

import raft4s.internal.RpcClientProvider
import raft4s.protocol.{AppendEntries, AppendEntriesResponse, VoteRequest, VoteResponse}
import raft4s.rpc.{RpcClient, RpcClientBuilder}
import raft4s.storage.Snapshot
import raft4s.{Command, LogEntry, Node}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

private[future] class RpcClientProviderImpl(clients: Map[Node, RpcClient[Future]])(implicit
  EC: ExecutionContext,
  builder: RpcClientBuilder[Future]
) extends RpcClientProvider[Future] {

  private val clientsRef = new AtomicReference[Map[Node, RpcClient[Future]]](clients)

  override def send(serverId: Node, voteRequest: VoteRequest): Future[VoteResponse] =
    getClient(serverId).send(voteRequest)

  override def send(serverId: Node, appendEntries: AppendEntries): Future[AppendEntriesResponse] =
    getClient(serverId).send(appendEntries)

  override def send(serverId: Node, snapshot: Snapshot, lastEntry: LogEntry): Future[AppendEntriesResponse] =
    getClient(serverId).send(snapshot, lastEntry)

  override def send[T](serverId: Node, command: Command[T]): Future[T] =
    getClient(serverId).send(command)

  override def join(serverId: Node, newNode: Node): Future[Boolean] =
    getClient(serverId).join(newNode)

  override def closeConnections(): Future[Unit] =
    Future.sequence(clientsRef.get().map(_._2.close())).map(_ => ())

  private def getClient(serverId: Node): RpcClient[Future] = {
    val clients = clientsRef.get()

    clients.get(serverId) match {
      case Some(client) => client
      case None =>
        val client = builder.build(serverId)
        clientsRef.set(clients + (serverId -> client))
        client
    }
  }
}

object RpcClientProviderImpl {
  def build(implicit EC: ExecutionContext, builder: RpcClientBuilder[Future]): RpcClientProvider[Future] =
    new RpcClientProviderImpl(Map.empty)
}
