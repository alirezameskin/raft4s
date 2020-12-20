package raft4s

import raft4s.protocol.LogState

trait LogCompactionPolicy[F[_]] {
  def eligible(state: LogState, stateMachine: StateMachine[F]): F[Boolean]
}
