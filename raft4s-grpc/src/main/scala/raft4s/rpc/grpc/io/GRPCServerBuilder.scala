package raft4s.rpc.grpc.io

import _root_.io.grpc.{Server, ServerBuilder}
import cats.effect.{IO, Resource}
import io.odin.Logger
import raft4s.grpc.protos
import raft4s.rpc.grpc.io.internal.GRPCRaftService
import raft4s.rpc.{RpcServer, RpcServerBuilder}
import raft4s.{Address, Raft}

import java.util.concurrent.TimeUnit
import scala.concurrent.blocking

class GRPCServerBuilder(implicit L: Logger[IO]) extends RpcServerBuilder[IO] {

  override def resource(address: Address, raft: Raft[IO]): Resource[IO, RpcServer[IO]] = {

    val service = protos.RaftGrpc.bindService(
      new GRPCRaftService(raft),
      scala.concurrent.ExecutionContext.global
    );

    val builder: ServerBuilder[_] = ServerBuilder
      .forPort(address.port)
      .addService(service)

    val acquire = IO.delay(builder.build().start())
    val shutdown: Server => IO[Unit] = server =>
      IO.delay {
        server.shutdown()
        if (!blocking(server.awaitTermination(30, TimeUnit.SECONDS))) {
          server.shutdownNow()
          ()
        }
      }

    Resource.make[IO, Server](acquire)(shutdown).map { server =>
      new RpcServer[IO] {
        override def start(): IO[Unit] = IO.unit
      }
    }
  }
}
