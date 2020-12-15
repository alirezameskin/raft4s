package raft4s.log

import raft4s.StateMachine

trait LogCompactionPolicy[F[_]] {
  def eligible(state: LogState, stateMachine: StateMachine[F]): F[Boolean]
}
