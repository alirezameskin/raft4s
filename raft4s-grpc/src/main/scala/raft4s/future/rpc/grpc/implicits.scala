package raft4s.future.rpc.grpc

import cats.effect.IO
import raft4s.internal.Logger

object implicits {

  implicit def clientBuilder(implicit L: Logger[IO]) = new GRPCClientBuilder
  implicit def serverBuilder(implicit L: Logger[IO]) = new GRPCServerBuilder
}
