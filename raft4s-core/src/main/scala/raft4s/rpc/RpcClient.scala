package raft4s.rpc

import raft4s.{Command, LogEntry, Node}
import raft4s.protocol._
import raft4s.storage.Snapshot

trait RpcClient[F[_]] {
  def send(voteRequest: VoteRequest): F[VoteResponse]

  def send(appendEntries: AppendEntries): F[AppendEntriesResponse]

  def send[T](command: Command[T]): F[T]

  def send(snapshot: Snapshot, lastEntry: LogEntry): F[AppendEntriesResponse]

  def join(server: Node): F[Boolean]

  def close(): F[Unit]
}
