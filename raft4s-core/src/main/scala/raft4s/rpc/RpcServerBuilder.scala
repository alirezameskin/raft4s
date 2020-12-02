package raft4s.rpc

import raft4s.{Address, Raft}

trait RpcServerBuilder[F[_]] {
  def build(address: Address, raft: Raft[F]): F[RpcServer[F]]
}

object RpcServerBuilder {
  def apply[F[_]](implicit builder: RpcServerBuilder[F]): RpcServerBuilder[F] = builder
}
