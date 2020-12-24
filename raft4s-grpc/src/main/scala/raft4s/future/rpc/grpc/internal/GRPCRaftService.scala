package raft4s.future.rpc.grpc.internal

import raft4s._
import raft4s.grpc.protos
import raft4s.internal.{Logger, Raft}
import raft4s.protocol._
import raft4s.rpc.internal.ObjectSerializer
import raft4s.storage.Snapshot

import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}

private[grpc] class GRPCRaftService(raft: Raft[Future])(implicit val logger: Logger[Future], EC: ExecutionContext)
    extends protos.RaftGrpc.Raft {

  override def vote(request: protos.VoteRequest): Future[protos.VoteResponse] =
    raft
      .onReceive(VoteRequest(toNode(request.nodeId), request.currentTerm, request.logLength, request.logTerm))
      .map(res => protos.VoteResponse(res.nodeId.id, res.term, res.voteGranted))

  override def appendEntries(request: protos.AppendEntriesRequest): Future[protos.AppendEntriesResponse] =
    raft
      .onReceive(
        AppendEntries(
          toNode(request.leaderId),
          request.term,
          request.logLength,
          request.logTerm,
          request.leaderCommit,
          request.entries
            .map(entry => LogEntry(entry.term, entry.index, ObjectSerializer.decode[Command[_]](entry.command)))
            .toList
        )
      )
      .map(response => protos.AppendEntriesResponse(response.nodeId.id, response.currentTerm, response.ack, response.success))

  override def execute(request: protos.CommandRequest): Future[protos.CommandResponse] =
    raft
      .onCommand(ObjectSerializer.decode[Command[Any]](request.command))
      .map(response => protos.CommandResponse(ObjectSerializer.encode[Any](response)))

  override def installSnapshot(request: protos.InstallSnapshotRequest): Future[protos.AppendEntriesResponse] =
    raft
      .onReceive(
        InstallSnapshot(
          Snapshot(
            request.lastIndexId,
            ByteBuffer.wrap(request.bytes.toByteArray),
            ObjectSerializer.decode[ClusterConfiguration](request.config)
          ),
          request.lastEntry
            .map(entry => LogEntry(entry.term, entry.index, ObjectSerializer.decode[Command[_]](entry.command)))
            .get
        )
      )
      .map(response => protos.AppendEntriesResponse(response.nodeId.id, response.currentTerm, response.ack, response.success))

  override def join(request: protos.JoinRequest): Future[protos.JoinResponse] =
    raft
      .addMember(Node(request.host, request.port))
      .map(_ => protos.JoinResponse())

  private def toNode(str: String): Node = Node.fromString(str).get //TODO
}
