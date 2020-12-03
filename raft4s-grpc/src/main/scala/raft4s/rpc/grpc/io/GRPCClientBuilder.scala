package raft4s.rpc.grpc.io

import _root_.io.grpc.ManagedChannelBuilder
import cats.effect.IO
import raft4s.Address
import raft4s.grpc.protos
import raft4s.grpc.protos.RaftGrpc
import raft4s.protocol._
import raft4s.rpc.grpc.io.internal.GRPCRaftClient
import raft4s.rpc.{RpcClient, RpcClientBuilder}

class GRPCClientBuilder extends RpcClientBuilder[IO] {

  override def build(address: Address): RpcClient[IO] = {

    implicit val EC = scala.concurrent.ExecutionContext.global
    implicit val CS = IO.contextShift(scala.concurrent.ExecutionContext.global)

    val channel = ManagedChannelBuilder.forAddress(address.host, address.port).usePlaintext().build()
    val stub    = protos.RaftGrpc.stub(channel)

    new GRPCRaftClient(stub)
  }
}
