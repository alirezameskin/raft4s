package raft4s.rpc.grpc.io.internal

import cats.effect.{ContextShift, IO}
import io.odin.Logger
import raft4s.Address
import raft4s.grpc.protos
import raft4s.grpc.protos.RaftGrpc
import raft4s.protocol._
import raft4s.rpc.RpcClient

import scala.concurrent.ExecutionContext

private[grpc] class GRPCRaftClient(address: Address, stub: RaftGrpc.RaftStub)(implicit
  CS: ContextShift[IO],
  EC: ExecutionContext,
  logger: Logger[IO]
) extends RpcClient[IO] {

  override def send(req: VoteRequest): IO[VoteResponse] = {
    val request  = protos.VoteRequest(req.nodeId, req.currentTerm, req.logLength, req.logTerm)
    val response = stub.vote(request).map(res => VoteResponse(res.nodeId, res.term, res.granted))

    IO
      .fromFuture(IO(response))
      .handleErrorWith { error =>
        logger.warn(s"An error in sending VoteRequest to node: ${address}, Error: ${error.getMessage}") *> IO
          .raiseError(error)
      }
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

    IO
      .fromFuture(IO(response))
      .handleErrorWith { error =>
        logger.warn(s"An error in sending AppendEntries request to node: ${address}, Error: ${error.getMessage}") *> IO
          .raiseError(error)
      }
  }

  override def send[T](command: Command[T]): IO[T] = {
    val request  = protos.CommandRequest(ObjectSerializer.encode[Command[T]](command))
    val response = stub.execute(request).map(response => ObjectSerializer.decode[T](response.outpue))

    IO
      .fromFuture(IO(response))
      .handleErrorWith { error =>
        logger.warn(s"An error in sending a command to node: ${address}. Command: ${command}, Error: ${error.getMessage}") *> IO
          .raiseError(error)
      }
  }
}
