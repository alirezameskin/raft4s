package raft4s.effect.rpc.grpc.io

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import raft4s.internal.Logger
import raft4s.rpc.grpc.serializer.JavaSerializer

object implicits {
  implicit val serializer                                          = new JavaSerializer
  implicit def clientBuilder(implicit L: Logger[IO], R: IORuntime) = new GRPCClientBuilder
  implicit def serverBuilder(implicit L: Logger[IO], R: IORuntime) = new GRPCServerBuilder
}
