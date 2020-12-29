package raft4s.internal

import cats.{Applicative, MonadError}
import raft4s.storage.{LogStorage, SnapshotStorage}
import raft4s.{LogCompactionPolicy, StateMachine}

class SimpleLog[F[_]: Applicative](
  val logStorage: LogStorage[F],
  val snapshotStorage: SnapshotStorage[F],
  val stateMachine: StateMachine[F],
  val membershipManager: MembershipManager[F],
  val compactionPolicy: LogCompactionPolicy[F],
  var lastCommit: Long
)(implicit val ME: MonadError[F, Throwable], val logger: Logger[F])
    extends Log[F] {
  override def transactional[A](t: => F[A]): F[A] =
    t

  override def getCommitIndex: F[Long] =
    Applicative[F].pure(lastCommit)

  override def setCommitIndex(index: Long): F[Unit] =
    Applicative[F].pure {
      lastCommit = index
    }
}
