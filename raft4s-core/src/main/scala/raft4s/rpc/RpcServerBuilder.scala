package raft4s.rpc

import cats.effect.Resource
import raft4s.{Node, Raft}

trait RpcServerBuilder[F[_]] {
  def resource(node: Node, raft: Raft[F]): Resource[F, RpcServer[F]]
}

object RpcServerBuilder {
  def apply[F[_]](implicit builder: RpcServerBuilder[F]): RpcServerBuilder[F] = builder
}
