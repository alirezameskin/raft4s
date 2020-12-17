package raft4s.rpc

import raft4s.protocol.Node

trait RpcClientBuilder[F[_]] {
  def build(address: Node): RpcClient[F]
}
