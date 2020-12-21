package raft4s.future.rpc.grpc

import raft4s.Node
import raft4s.rpc.{RpcClient, RpcClientBuilder}

import scala.concurrent.Future

class GRPCClientBuilder extends RpcClientBuilder[Future] {
  override def build(address: Node): RpcClient[Future] = ???
}
