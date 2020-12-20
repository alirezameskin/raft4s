package raft4s

import cats.Applicative
import raft4s.protocol.LogState

case class FixedSizeLogCompaction[F[_]: Applicative](size: Int) extends LogCompactionPolicy[F] {

  override def eligible(state: LogState, stateMachine: StateMachine[F]): F[Boolean] =
    if (state.lastAppliedIndex > size && state.lastAppliedIndex % size == 0)
      Applicative[F].pure(true)
    else
      Applicative[F].pure(false)
}
