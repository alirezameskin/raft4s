package raft4s.effect.rpc.grpc.io

import cats.effect.IO
import raft4s.internal.Logger

object implicits {
  implicit def clientBuilder(implicit L: Logger[IO]) = new GRPCClientBuilder
  implicit def serverBuilder(implicit L: Logger[IO]) = new GRPCServerBuilder
}
