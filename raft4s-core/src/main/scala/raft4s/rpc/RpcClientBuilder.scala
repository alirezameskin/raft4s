package raft4s.rpc

import raft4s.Server

trait RpcClientBuilder[F[_]] {
  def build(server: Server): RpcClient[F]
}
