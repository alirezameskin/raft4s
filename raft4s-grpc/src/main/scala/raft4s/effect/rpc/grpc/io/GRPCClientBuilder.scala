package raft4s.effect.rpc.grpc.io

import _root_.io.grpc.ManagedChannelBuilder
import cats.effect.{ContextShift, IO}
import raft4s.Node
import raft4s.effect.rpc.grpc.io.internal.GRPCRaftClient
import raft4s.internal.Logger
import raft4s.rpc.{RpcClient, RpcClientBuilder}

class GRPCClientBuilder(implicit L: Logger[IO], CS: ContextShift[IO]) extends RpcClientBuilder[IO] {

  override def build(node: Node): RpcClient[IO] = {

    val builder: ManagedChannelBuilder[_] = ManagedChannelBuilder
      .forAddress(node.host, node.port)
      .disableRetry()
      .usePlaintext()

    new GRPCRaftClient(node, builder.build())
  }
}
