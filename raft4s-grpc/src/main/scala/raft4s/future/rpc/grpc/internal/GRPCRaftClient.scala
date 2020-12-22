package raft4s.future.rpc.grpc.internal

import com.google.protobuf
import io.grpc.ManagedChannel
import raft4s._
import raft4s.grpc.protos
import raft4s.grpc.protos.JoinRequest
import raft4s.internal.Logger
import raft4s.protocol._
import raft4s.rpc.RpcClient
import raft4s.rpc.internal.ObjectSerializer
import raft4s.storage.Snapshot

import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future, blocking}

private[grpc] class GRPCRaftClient(address: Node, channel: ManagedChannel)(implicit
  EC: ExecutionContext,
  logger: Logger[Future]
) extends RpcClient[Future] {

  val stub = protos.RaftGrpc.stub(channel)

  override def send(req: VoteRequest): Future[VoteResponse] = {
    val request = protos.VoteRequest(req.nodeId.id, req.term, req.lastLogIndex, req.lastLogTerm)

      stub
        .vote(request)
        .map(res => VoteResponse(toNode(res.nodeId), res.term, res.granted))

  }

  override def send(appendEntries: AppendEntries): Future[AppendEntriesResponse] = {
    val request = protos.AppendEntriesRequest(
      appendEntries.leaderId.id,
      appendEntries.term,
      appendEntries.prevLogIndex,
      appendEntries.prevLogTerm,
      appendEntries.leaderCommit,
      appendEntries.entries.map(entry =>
        protos.LogEntry(entry.term, entry.index, ObjectSerializer.encode[Command[_]](entry.command))
      )
    )

      stub
        .appendEntries(request)
        .map(res => AppendEntriesResponse(toNode(res.nodeId), res.currentTerm, res.ack, res.success))
  }

  override def send[T](command: Command[T]): Future[T] = {
    val request  = protos.CommandRequest(ObjectSerializer.encode[Command[T]](command))
stub.execute(request).map(response => ObjectSerializer.decode[T](response.output))

  }

  override def send(snapshot: Snapshot, lastEntry: LogEntry): Future[AppendEntriesResponse] = {
    val request =
      protos.InstallSnapshotRequest(
        snapshot.lastIndex,
        Some(protos.LogEntry(lastEntry.term, lastEntry.index, ObjectSerializer.encode[Command[_]](lastEntry.command))),
        protobuf.ByteString.copyFrom(snapshot.bytes.array()),
        ObjectSerializer.encode[ClusterConfiguration](snapshot.config)
      )
stub
      .installSnapshot(request)
      .map(res => AppendEntriesResponse(toNode(res.nodeId), res.currentTerm, res.ack, res.success))

  }

  override def join(node: Node): Future[Boolean] =
    stub.join(JoinRequest(node.host, node.port)).map(_ => true)

  override def close(): Future[Unit] =
    Future{
      channel.shutdown()
      if (!blocking(channel.awaitTermination(30, TimeUnit.SECONDS))) {
        channel.shutdownNow()
        ()
      }
    }

  private def toNode(str: String): Node = Node.fromString(str).get //TODO

  private def toNode(info: protos.NodeInfo): Node = Node(info.host, info.port)
}
