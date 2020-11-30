package raft4s.rpc

import cats.effect.IO
import raft4s.Raft

trait RpcServer[F[_]] {
  def receive(voteRequest: VoteRequest): F[VoteResponse]

  def receive(appendEntries: AppendEntries): F[AppendEntriesResponse]
}

object RpcServer {
  def apply(raft: Raft[IO]): RpcServer[IO] = new RpcServer[IO]() {
    override def receive(voteRequest: VoteRequest): IO[VoteResponse] = raft.onReceive(voteRequest)

    override def receive(appendEntries: AppendEntries): IO[AppendEntriesResponse] = raft.onReceive(appendEntries)
  }
}
