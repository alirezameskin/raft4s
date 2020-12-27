package raft4s.effect.rpc.grpc.io.internal

import cats.effect.IO
import raft4s.grpc.protos
import raft4s.internal.{Logger, Raft}
import raft4s.protocol._
import raft4s.rpc.grpc.serializer.Serializer
import raft4s.storage.Snapshot
import raft4s.{Command, LogEntry, Node}

import java.nio.ByteBuffer
import scala.concurrent.Future

private[grpc] class GRPCRaftService(raft: Raft[IO], serializer: Serializer)(implicit val logger: Logger[IO])
    extends protos.RaftGrpc.Raft {

  override def vote(request: protos.VoteRequest): Future[protos.VoteResponse] =
    raft
      .onReceive(VoteRequest(toNode(request.nodeId), request.currentTerm, request.logLength, request.logTerm))
      .map(res => protos.VoteResponse(res.nodeId.id, res.term, res.voteGranted))
      .handleErrorWith { error =>
        logger.warn(s"Error during the VoteRequest process. Error ${error.getMessage}") *> IO.raiseError(error)
      }
      .unsafeToFuture()

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
            .map(entry => LogEntry(entry.term, entry.index, serializer.decode[Command[_]](entry.command)))
            .toList
        )
      )
      .map(response => protos.AppendEntriesResponse(response.nodeId.id, response.currentTerm, response.ack, response.success))
      .unsafeToFuture()

  override def execute(request: protos.CommandRequest): Future[protos.CommandResponse] =
    raft
      .onCommand(serializer.decode[Command[Any]](request.command))
      .map(response => protos.CommandResponse(serializer.encode[Any](response)))
      .handleErrorWith { error =>
        logger.warn(s"An error during the command process. Error ${error.getMessage}") *> IO.raiseError(error)
      }
      .unsafeToFuture()

  override def installSnapshot(request: protos.InstallSnapshotRequest): Future[protos.AppendEntriesResponse] =
    raft
      .onReceive(
        InstallSnapshot(
          Snapshot(
            request.lastIndexId,
            ByteBuffer.wrap(request.bytes.toByteArray),
            serializer.decode[ClusterConfiguration](request.config)
          ),
          request.lastEntry
            .map(entry => LogEntry(entry.term, entry.index, serializer.decode[Command[_]](entry.command)))
            .get
        )
      )
      .map(response => protos.AppendEntriesResponse(response.nodeId.id, response.currentTerm, response.ack, response.success))
      .handleErrorWith { error =>
        logger.warn(s"An error during snapshot installation. Error ${error.getMessage}") *> IO.raiseError(error)
      }
      .unsafeToFuture()

  override def join(request: protos.JoinRequest): Future[protos.JoinResponse] =
    raft
      .addMember(Node(request.host, request.port))
      .map(_ => protos.JoinResponse())
      .unsafeToFuture()

  private def toNode(str: String): Node = Node.fromString(str).get //TODO
}
