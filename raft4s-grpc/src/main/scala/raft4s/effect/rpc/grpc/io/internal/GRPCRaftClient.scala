package raft4s.effect.rpc.grpc.io.internal

import cats.effect.{ContextShift, IO}
import com.google.protobuf
import io.grpc.ManagedChannel
import raft4s.grpc.protos
import raft4s.grpc.protos.JoinRequest
import raft4s.internal.Logger
import raft4s.protocol._
import raft4s.rpc.RpcClient
import raft4s.rpc.grpc.serializer.Serializer
import raft4s.storage.Snapshot
import raft4s.{Command, LogEntry, Node}

import java.util.concurrent.TimeUnit
import scala.concurrent.blocking

private[grpc] class GRPCRaftClient(address: Node, channel: ManagedChannel, serializer: Serializer)(implicit
  CS: ContextShift[IO],
  logger: Logger[IO]
) extends RpcClient[IO] {

  val stub = protos.RaftGrpc.stub(channel)

  override def send(req: VoteRequest): IO[VoteResponse] = {
    val request  = protos.VoteRequest(req.nodeId.id, req.term, req.lastLogIndex, req.lastLogTerm)
    val response = stub.vote(request)

    IO
      .fromFuture(IO(response))
      .map(res => VoteResponse(toNode(res.nodeId), res.term, res.granted))
      .handleErrorWith { error =>
        logger.warn(s"An error in sending VoteRequest to node: ${address}, Error: ${error.getMessage}") *> IO
          .raiseError(error)
      }
  }

  override def send(appendEntries: AppendEntries): IO[AppendEntriesResponse] = {
    val request = protos.AppendEntriesRequest(
      appendEntries.leaderId.id,
      appendEntries.term,
      appendEntries.prevLogIndex,
      appendEntries.prevLogTerm,
      appendEntries.leaderCommit,
      appendEntries.entries.map(entry => protos.LogEntry(entry.term, entry.index, serializer.encode[Command[_]](entry.command)))
    )

    val response = stub.appendEntries(request)

    IO
      .fromFuture(IO(response))
      .map(res => AppendEntriesResponse(toNode(res.nodeId), res.currentTerm, res.ack, res.success))
      .handleErrorWith { error =>
        logger.warn(s"An error in sending AppendEntries request to node: ${address}, Error: ${error.getMessage}") *> IO
          .raiseError(error)
      }
  }

  override def send[T](command: Command[T]): IO[T] = {
    val request  = protos.CommandRequest(serializer.encode[Command[T]](command))
    val response = stub.execute(request)

    IO
      .fromFuture(IO(response))
      .map(response => serializer.decode[T](response.output))
      .handleErrorWith { error =>
        logger.warn(s"An error in sending a command to node: ${address}. Command: ${command}, Error: ${error.getMessage}") *> IO
          .raiseError(error)
      }
  }

  override def send(snapshot: Snapshot, lastEntry: LogEntry): IO[AppendEntriesResponse] = {
    val request =
      protos.InstallSnapshotRequest(
        snapshot.lastIndex,
        Some(protos.LogEntry(lastEntry.term, lastEntry.index, serializer.encode[Command[_]](lastEntry.command))),
        protobuf.ByteString.copyFrom(snapshot.bytes.array()),
        serializer.encode[ClusterConfiguration](snapshot.config)
      )

    val response = stub.installSnapshot(request)

    IO
      .fromFuture(IO(response))
      .map(res => AppendEntriesResponse(toNode(res.nodeId), res.currentTerm, res.ack, res.success))
      .handleErrorWith { error =>
        logger.warn(
          s"An error in sending a snapshot to node: ${address}. Snapshot: ${snapshot}, Error: ${error.getMessage}"
        ) *> IO
          .raiseError(error)
      }
  }

  override def join(node: Node): IO[Boolean] =
    IO
      .fromFuture(IO(stub.join(JoinRequest(node.host, node.port))))
      .map(_ => true)

  override def close(): IO[Unit] =
    IO.delay {
      channel.shutdown()
      if (!blocking(channel.awaitTermination(30, TimeUnit.SECONDS))) {
        channel.shutdownNow()
        ()
      }
    }

  private def toNode(str: String): Node = Node.fromString(str).get //TODO

  private def toNode(info: protos.NodeInfo): Node = Node(info.host, info.port)
}
