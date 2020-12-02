package raft4s.rpc.grpc.io

object implicits {
  implicit val clientBuilder = new GRPCClientBuilder
  implicit val serverBuilder = new GRPCServerBuilder
}
