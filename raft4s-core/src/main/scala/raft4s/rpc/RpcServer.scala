package raft4s.rpc

import cats.effect.IO
import raft4s.Raft

trait RpcServer {
  def receive(voteRequest: VoteRequest): IO[VoteResponse]

  def receive(appendEntries: AppendEntries): IO[AppendEntriesResponse]
}

object RpcServer {
  def apply(raft: Raft): RpcServer = new RpcServer() {
    override def receive(voteRequest: VoteRequest): IO[VoteResponse] = raft.onReceive(voteRequest)

    override def receive(appendEntries: AppendEntries): IO[AppendEntriesResponse] = raft.onReceive(appendEntries)
  }
}
