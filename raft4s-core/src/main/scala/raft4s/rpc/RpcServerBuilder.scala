package raft4s.rpc

import raft4s.{Node, Raft}

import scala.annotation.implicitNotFound

@implicitNotFound("""
Could not find an instance of RpcServerBuilder for ${F}.
You might add a custom server builder for ${F}.
If you are using cats.effect.IO, class you can import the default one.
import raft4s.effect.rpc.grpc.io.implicits._
""")
trait RpcServerBuilder[F[_]] {
  def build(node: Node, raft: Raft[F]): F[RpcServer[F]]
}

object RpcServerBuilder {
  def apply[F[_]](implicit builder: RpcServerBuilder[F]): RpcServerBuilder[F] = builder
}
