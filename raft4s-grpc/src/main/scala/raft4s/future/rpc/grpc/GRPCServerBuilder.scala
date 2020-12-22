package raft4s.future.rpc.grpc

import raft4s.Node
import raft4s.internal.Raft
import raft4s.rpc.{RpcServer, RpcServerBuilder}

import scala.concurrent.Future

class GRPCServerBuilder extends RpcServerBuilder[Future] {

  override def build(node: Node, raft: Raft[Future]): Future[RpcServer[Future]] = ???
}
