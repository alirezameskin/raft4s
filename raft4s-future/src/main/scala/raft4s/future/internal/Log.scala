package raft4s.future.internal

import cats.MonadError
import raft4s.internal.{Logger, MembershipManager}
import raft4s.storage.{LogStorage, SnapshotStorage}
import raft4s.{LogCompactionPolicy, StateMachine}

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future}

class Log(
  val logStorage: LogStorage[Future],
  val snapshotStorage: SnapshotStorage[Future],
  val stateMachine: StateMachine[Future],
  val membershipManager: MembershipManager[Future],
  val compactionPolicy: LogCompactionPolicy[Future],
  val lastCommit: Long
)(implicit val ME: MonadError[Future, Throwable], val logger: Logger[Future])
    extends raft4s.internal.Log[Future] {

  private val lastCommitIndex = new AtomicLong(lastCommit)

  override def withPermit[A](t: Future[A]): Future[A] =
    //???
    t

  override def getCommitIndex: Future[Long] =
    Future.successful(lastCommitIndex.get())

  override def setCommitIndex(index: Long): Future[Unit] =
    Future.successful(lastCommitIndex.set(index))
}

object Log {
  def build(
    logStorage: LogStorage[Future],
    snapshotStorage: SnapshotStorage[Future],
    stateMachine: StateMachine[Future],
    compactionPolicy: LogCompactionPolicy[Future],
    membershipManager: MembershipManager[Future],
    lastCommitIndex: Long
  )(implicit L: Logger[Future], EX: ExecutionContext): raft4s.internal.Log[Future] =
    new Log(
      logStorage,
      snapshotStorage,
      stateMachine,
      membershipManager,
      compactionPolicy,
      lastCommitIndex
    )
}
