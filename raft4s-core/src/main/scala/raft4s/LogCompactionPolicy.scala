package raft4s

import cats.Applicative
import raft4s.protocol.LogState

trait LogCompactionPolicy[F[_]] {
  def eligible(state: LogState, stateMachine: StateMachine[F]): F[Boolean]
}

object LogCompactionPolicy {

  def noCompaction[F[_]: Applicative]: LogCompactionPolicy[F] = new LogCompactionPolicy[F] {
    override def eligible(state: LogState, stateMachine: StateMachine[F]): F[Boolean] =
      Applicative[F].pure(false)
  }

  def fixedSize[F[_]: Applicative](size: Int): LogCompactionPolicy[F] = new LogCompactionPolicy[F] {
    override def eligible(state: LogState, stateMachine: StateMachine[F]): F[Boolean] =
      if (state.lastAppliedIndex > size && state.lastAppliedIndex % size == 0)
        Applicative[F].pure(true)
      else
        Applicative[F].pure(false)
  }
}
