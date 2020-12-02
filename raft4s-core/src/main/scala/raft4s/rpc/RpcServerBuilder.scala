package raft4s.rpc

import raft4s.Raft

trait RpcServerBuilder[F[_]] {
  def build(raft: Raft[F]): RpcServer[F]
}
