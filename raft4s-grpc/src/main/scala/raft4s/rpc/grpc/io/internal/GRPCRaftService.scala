package raft4s.rpc.grpc.io.internal

import cats.effect.IO
import raft4s.Raft
import raft4s.grpc.protos
import raft4s.grpc.protos.{CommandRequest, CommandResponse}
import raft4s.protocol.{AppendEntries, Command, LogEntry, VoteRequest}

import scala.concurrent.Future

private[grpc] class GRPCRaftService(raft: Raft[IO]) extends protos.RaftGrpc.Raft {

  override def vote(request: protos.VoteRequest): Future[protos.VoteResponse] =
    raft
      .onReceive(VoteRequest(request.nodeId, request.currentTerm, request.logLength, request.logTerm))
      .map(res => protos.VoteResponse(res.nodeId, res.term, res.granted))
      .unsafeToFuture()

  override def appendEntries(request: protos.AppendEntriesRequest): Future[protos.AppendEntriesResponse] =
    raft
      .onReceive(
        AppendEntries(
          request.leaderId,
          request.term,
          request.logLength,
          request.logTerm,
          request.leaderCommit,
          request.entries
            .map(entry => LogEntry(entry.term, entry.index, ObjectSerializer.decode[Command[_]](entry.command)))
            .toList
        )
      )
      .map(response => protos.AppendEntriesResponse(response.nodeId, response.currentTerm, response.ack, response.success))
      .unsafeToFuture()

  override def execute(request: CommandRequest): Future[CommandResponse] =
    raft
      .onCommand(ObjectSerializer.decode[Command[Any]](request.command))
      .map(response => protos.CommandResponse(ObjectSerializer.encode[Any](response)))
      .unsafeToFuture()
}
