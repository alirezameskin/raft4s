package raft4s.future.rpc.grpc

import raft4s.internal.Logger

import scala.concurrent.{ExecutionContext, Future}

object implicits {

  implicit def clientBuilder(implicit EC: ExecutionContext, L: Logger[Future]) = new GRPCClientBuilder
  implicit def serverBuilder(implicit EC: ExecutionContext, L: Logger[Future]) = new GRPCServerBuilder
}
