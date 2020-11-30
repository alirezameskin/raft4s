package raft4s.rpc

trait RpcClient[F[_]] {
  def send(voteRequest: VoteRequest): F[VoteResponse]

  def send(appendEntries: AppendEntries): F[AppendEntriesResponse]
}
