package raft4s.rpc.grpc.io

import _root_.io.grpc.ServerBuilder
import cats.effect.IO
import raft4s.grpc.protos
import raft4s.rpc.grpc.io.internal.GRPCRaftService
import raft4s.rpc.{RpcServer, RpcServerBuilder}
import raft4s.{Address, Raft}

class GRPCServerBuilder extends RpcServerBuilder[IO] {
  override def build(address: Address, raft: Raft[IO]): IO[RpcServer[IO]] = {

    val service = protos.RaftGrpc.bindService(
      new GRPCRaftService(raft),
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
