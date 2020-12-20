package raft4s.effect.rpc.grpc.io

import _root_.io.grpc.ServerBuilder
import cats.effect.IO
import raft4s.effect.rpc.grpc.io.internal.GRPCRaftService
import raft4s.grpc.protos
import raft4s.internal.Logger
import raft4s.rpc.{RpcServer, RpcServerBuilder}
import raft4s.{Node, Raft}

import java.util.concurrent.TimeUnit
import scala.concurrent.blocking

class GRPCServerBuilder(implicit L: Logger[IO]) extends RpcServerBuilder[IO] {

  override def build(node: Node, raft: Raft[IO]): IO[RpcServer[IO]] =
    IO.delay {
      val service = protos.RaftGrpc.bindService(
        new GRPCRaftService(raft),
        scala.concurrent.ExecutionContext.global
      );

      val builder: ServerBuilder[_] = ServerBuilder
        .forPort(node.port)
        .addService(service)

      val server = builder.build()

      new RpcServer[IO] {
        override def start: IO[Unit] =
          IO.delay(server.start()) *> IO.unit

        override def stop: IO[Unit] =
          IO.delay {
            server.shutdown()
            if (!blocking(server.awaitTermination(30, TimeUnit.SECONDS))) {
              server.shutdownNow()
              ()
            }
          }
      }
    }
}
