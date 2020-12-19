package raft4s.rpc

import raft4s.Node

trait RpcClientBuilder[F[_]] {
  def build(address: Node): RpcClient[F]
}
