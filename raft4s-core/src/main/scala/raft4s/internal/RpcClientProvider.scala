package raft4s.internal

import raft4s.Node
import raft4s.protocol._
import raft4s.storage.Snapshot

private[raft4s] trait RpcClientProvider[F[_]] {

  def send(serverId: Node, voteRequest: VoteRequest): F[VoteResponse]

  def send(serverId: Node, appendEntries: AppendEntries): F[AppendEntriesResponse]

  def send(serverId: Node, snapshot: Snapshot, lastEntry: LogEntry): F[AppendEntriesResponse]

  def send[T](serverId: Node, command: Command[T]): F[T]

  def join(serverId: Node, newNode: Node): F[Boolean]

  def closeConnections(): F[Unit]

}
