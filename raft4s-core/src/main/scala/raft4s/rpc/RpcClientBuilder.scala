package raft4s.rpc

import raft4s.Address

trait RpcClientBuilder[F[_]] {
  def build(address: Address): RpcClient[F]
}
