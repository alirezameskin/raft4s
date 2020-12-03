package raft4s.rpc.grpc.io.internal

import cats.effect.{ContextShift, IO}
import raft4s.grpc.protos
import raft4s.grpc.protos.RaftGrpc
import raft4s.protocol._
import raft4s.rpc.RpcClient

import scala.concurrent.ExecutionContext

private[grpc] class GRPCRaftClient(stub: RaftGrpc.RaftStub)(implicit CS: ContextShift[IO], EC: ExecutionContext)
    extends RpcClient[IO] {

  override def send(voteRequest: VoteRequest): IO[VoteResponse] = {
    val request =
      protos.VoteRequest(voteRequest.nodeId, voteRequest.currentTerm, voteRequest.logLength, voteRequest.logTerm)

    val response = stub.vote(request).map(res => VoteResponse(res.nodeId, res.term, res.granted))

    IO.fromFuture(IO(response))
  }

  override def send(appendEntries: AppendEntries): IO[AppendEntriesResponse] = {
    val request = protos.AppendEntriesRequest(
      appendEntries.leaderId,
      appendEntries.term,
      appendEntries.logLength,
      appendEntries.logTerm,
      appendEntries.leaderCommit,
      appendEntries.entries.map(entry =>
        protos.LogEntry(entry.term, entry.index, ObjectSerializer.encode[Command[_]](entry.command))
      )
    )

    val response =
      stub.appendEntries(request).map(res => AppendEntriesResponse(res.nodeId, res.currentTerm, res.ack, res.success))

    IO.fromFuture(IO(response))
  }

  override def send[T](command: Command[T]): IO[T] = {
    val request  = protos.CommandRequest(ObjectSerializer.encode[Command[T]](command))
    val response = stub.execute(request).map(response => ObjectSerializer.decode[T](response.outpue))

    IO.fromFuture(IO(response))
  }
}
