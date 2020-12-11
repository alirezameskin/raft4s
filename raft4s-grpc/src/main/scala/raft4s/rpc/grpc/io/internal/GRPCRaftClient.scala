package raft4s.rpc.grpc.io.internal

import cats.effect.{ContextShift, IO, Timer}
import com.google.protobuf
import io.odin.Logger
import raft4s.Address
import raft4s.grpc.protos
import raft4s.grpc.protos.RaftGrpc
import raft4s.protocol._
import raft4s.rpc.RpcClient
import raft4s.storage.Snapshot
import scalapb.descriptors.ScalaType.ByteString

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

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
      appendEntries.leaderAppliedIndex,
      appendEntries.entries.map(entry =>
        protos.LogEntry(entry.term, entry.index, ObjectSerializer.encode[Command[_]](entry.command))
      )
    )

    val response =
      stub
        .appendEntries(request)
        .map(res => AppendEntriesResponse(res.nodeId, res.currentTerm, res.ack, res.success))

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

  override def send(snapshot: Snapshot, lastEntry: LogEntry): IO[AppendEntriesResponse] = {
    val request =
      protos.InstallSnapshotRequest(
        snapshot.lastIndex,
        Some(protos.LogEntry(lastEntry.term, lastEntry.index, ObjectSerializer.encode[Command[_]](lastEntry.command))),
        protobuf.ByteString.copyFrom(snapshot.bytes.array())
      )
    val response = stub
      .installSnapshot(request)
      .map(res => AppendEntriesResponse(res.nodeId, res.currentTerm, res.ack, res.success))

    IO
      .fromFuture(IO(response))
      .handleErrorWith { error =>
        logger.warn(
          s"An error in sending a snapshot to node: ${address}. Snapshot: ${snapshot}, Error: ${error.getMessage}"
        ) *> IO
          .raiseError(error)
      }
  }
}
