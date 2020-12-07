package raft4s.rpc.grpc.io

import cats.effect.IO
import io.odin.Logger

object implicits {
  implicit def clientBuilder(implicit L: Logger[IO]) = new GRPCClientBuilder
  implicit def serverBuilder(implicit L: Logger[IO]) = new GRPCServerBuilder
}
