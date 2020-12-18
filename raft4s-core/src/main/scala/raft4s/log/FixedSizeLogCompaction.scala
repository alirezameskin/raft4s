package raft4s.log
import cats.Applicative
import raft4s.StateMachine

case class FixedSizeLogCompaction[F[_]: Applicative](size: Int) extends LogCompactionPolicy[F] {

  override def eligible(state: LogState, stateMachine: StateMachine[F]): F[Boolean] =
    if (state.appliedIndex > size && state.appliedIndex % size == 0)
      Applicative[F].pure(true)
    else
      Applicative[F].pure(false)
}
