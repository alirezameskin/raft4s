package raft4s.rpc

import raft4s.protocol._

trait RpcClient[F[_]] {
  def send(voteRequest: VoteRequest): F[VoteResponse]

  def send(appendEntries: AppendEntries): F[AppendEntriesResponse]

  def send[T](command: Command[T]): F[T]
}
