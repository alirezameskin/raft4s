package raft4s.future.rpc.grpc

import io.grpc.ManagedChannelBuilder
import raft4s.Node
import raft4s.future.rpc.grpc.internal.GRPCRaftClient
import raft4s.internal.Logger
import raft4s.rpc.{RpcClient, RpcClientBuilder}

import scala.concurrent.{ExecutionContext, Future}

class GRPCClientBuilder(implicit EC: ExecutionContext, L: Logger[Future]) extends RpcClientBuilder[Future] {
  override def build(node: Node): RpcClient[Future] = {

    val builder: ManagedChannelBuilder[_] = ManagedChannelBuilder
      .forAddress(node.host, node.port)
      .usePlaintext()

    new GRPCRaftClient(node, builder.build())
  }
}
