package raft4s.internal

import raft4s.Node
import raft4s.protocol.AppendEntriesResponse

private[raft4s] trait LogPropagator[F[_]] {

  def propagateLogs(peerId: Node, term: Long, nextIndex: Long): F[AppendEntriesResponse]

}
