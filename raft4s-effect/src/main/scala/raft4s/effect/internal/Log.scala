package raft4s.effect.internal

import cats.effect.Concurrent
import cats.effect.concurrent.{Ref, Semaphore}
import cats.implicits._
import cats.{Monad, MonadError}
import raft4s.internal.{Logger, MembershipManager}
import raft4s.storage.{LogStorage, SnapshotStorage}
import raft4s.{internal, LogCompactionPolicy, StateMachine}

class Log[F[_]: Monad: Logger](
  val logStorage: LogStorage[F],
  val snapshotStorage: SnapshotStorage[F],
  val stateMachine: StateMachine[F],
  val membershipManager: MembershipManager[F],
  val compactionPolicy: LogCompactionPolicy[F],
  commitIndexRef: Ref[F, Long],
  semaphore: Semaphore[F]
)(implicit val ME: MonadError[F, Throwable], val logger: Logger[F])
    extends internal.Log[F] {

  override def withPermit[A](t: F[A]): F[A] =
    semaphore.withPermit(t)

  override def getCommitIndex: F[Long] =
    commitIndexRef.get

  override def setCommitIndex(index: Long): F[Unit] =
    commitIndexRef.set(index)
}

object Log {
  def build[F[_]: Concurrent: Logger](
    logStorage: LogStorage[F],
    snapshotStorage: SnapshotStorage[F],
    stateMachine: StateMachine[F],
    compactionPolicy: LogCompactionPolicy[F],
    membershipManager: MembershipManager[F],
    lastCommitIndex: Long
  ): F[Log[F]] =
    for {
      lock           <- Semaphore[F](1)
      commitIndexRef <- Ref.of[F, Long](lastCommitIndex)
    } yield new Log(
      logStorage,
      snapshotStorage,
      stateMachine,
      membershipManager,
      compactionPolicy,
      commitIndexRef,
      lock
    )
}
