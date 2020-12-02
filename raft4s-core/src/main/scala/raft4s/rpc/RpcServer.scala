package raft4s.rpc

import raft4s.protocol.{AppendEntries, AppendEntriesResponse, VoteRequest, VoteResponse}

trait RpcServer[F[_]] {
  def receive(voteRequest: VoteRequest): F[VoteResponse]

  def receive(appendEntries: AppendEntries): F[AppendEntriesResponse]
}
