package raft4s.rpc

import raft4s.protocol.{AppendEntries, AppendEntriesResponse, VoteRequest, VoteResponse}

trait RpcClient[F[_]] {
  def send(voteRequest: VoteRequest): F[VoteResponse]

  def send(appendEntries: AppendEntries): F[AppendEntriesResponse]
}
