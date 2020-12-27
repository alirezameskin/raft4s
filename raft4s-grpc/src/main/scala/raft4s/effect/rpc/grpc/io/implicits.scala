package raft4s.effect.rpc.grpc.io

import cats.effect.{ContextShift, IO}
import raft4s.internal.Logger
import raft4s.rpc.grpc.serializer.JavaSerializer

object implicits {
  implicit val serializer                                                  = new JavaSerializer
  implicit def clientBuilder(implicit L: Logger[IO], CS: ContextShift[IO]) = new GRPCClientBuilder
  implicit def serverBuilder(implicit L: Logger[IO], CS: ContextShift[IO]) = new GRPCServerBuilder
}
