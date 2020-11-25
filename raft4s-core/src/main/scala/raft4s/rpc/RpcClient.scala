package raft4s.rpc

import cats.effect.IO

trait RpcClient {
  def send(voteRequest: VoteRequest): IO[VoteResponse]

  def send(appendEntries: AppendEntries): IO[AppendEntriesResponse]
}
