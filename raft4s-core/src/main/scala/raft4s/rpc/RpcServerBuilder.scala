package raft4s.rpc

import raft4s.{Node, Raft}

trait RpcServerBuilder[F[_]] {
  def build(node: Node, raft: Raft[F]): F[RpcServer[F]]
}

object RpcServerBuilder {
  def apply[F[_]](implicit builder: RpcServerBuilder[F]): RpcServerBuilder[F] = builder
}
