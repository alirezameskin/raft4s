package raft4s.rpc.grpc.io

import java.io.ObjectInputStream

import _root_.io.grpc.ServerBuilder
import cats.effect.IO
import raft4s.grpc.protos
import raft4s.protocol.{AppendEntries, Command, LogEntry, VoteRequest}
import raft4s.rpc.{RpcServer, RpcServerBuilder}
import raft4s.{Address, Raft}

import scala.concurrent.Future

class GRPCServerBuilder extends RpcServerBuilder[IO] {
  override def build(address: Address, raft: Raft[IO]): IO[RpcServer[IO]] = {

    val service = protos.RaftGrpc.bindService(
      new protos.RaftGrpc.Raft {
        override def vote(request: protos.VoteRequest): Future[protos.VoteResponse] =
          raft
            .onReceive(VoteRequest(request.nodeId, request.currentTerm, request.logLength, request.logTerm))
            .map { res =>
              protos.VoteResponse(res.nodeId, res.term, res.granted)
            }
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
                request.entries.map { entry =>
                  val ois = new ObjectInputStream(entry.command.newInput)
                  val cmd = ois.readObject().asInstanceOf[Command[_]]
                  ois.close()

                  LogEntry(entry.term, entry.index, cmd)
                }.toList
              )
            )
            .map { response =>
              protos.AppendEntriesResponse(response.nodeId, response.currentTerm, response.ack, response.success)
            }
            .unsafeToFuture()
      },
      scala.concurrent.ExecutionContext.global
    );

    val builder: ServerBuilder[_] = ServerBuilder
      .forPort(address.port)
      .addService(service)

    val server = builder.build()

    IO(new RpcServer[IO] {
      override def start(): IO[Unit] = IO.pure(server.start()) *> IO.unit
    })

  }
}
