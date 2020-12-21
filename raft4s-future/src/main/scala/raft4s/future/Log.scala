package raft4s.future

import cats.MonadError
import raft4s.StateMachine
import raft4s.internal.{Logger, MembershipManager}
import raft4s.log.LogCompactionPolicy
import raft4s.storage.{LogStorage, SnapshotStorage}

import scala.concurrent.{ExecutionContext, Future}

class Log(
  val logStorage: LogStorage[Future],
  val snapshotStorage: SnapshotStorage[Future],
  val stateMachine: StateMachine[Future],
  val membershipManager: MembershipManager[Future],
  val compactionPolicy: LogCompactionPolicy[Future]
)(implicit val ME: MonadError[Future, Throwable], val logger: Logger[Future])
    extends raft4s.log.Log[Future] {

  override def withPermit[A](t: Future[A]): Future[A] = ???

  override def getCommitIndex: Future[Long] = ???

  override def setCommitIndex(index: Long): Future[Unit] = ???
}

object Log {
  def build(
    logStorage: LogStorage[Future],
    snapshotStorage: SnapshotStorage[Future],
    stateMachine: StateMachine[Future],
    compactionPolicy: LogCompactionPolicy[Future],
    membershipManager: MembershipManager[Future],
    lastCommitIndex: Long
  )(implicit L: Logger[Future], EX: ExecutionContext): raft4s.log.Log[Future] =
    new Log(
      logStorage,
      snapshotStorage,
      stateMachine,
      membershipManager,
      compactionPolicy
    )
}
