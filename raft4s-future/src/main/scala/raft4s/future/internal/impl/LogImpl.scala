package raft4s.future.internal.impl

import cats.MonadError
import raft4s.internal.{Log, Logger, MembershipManager}
import raft4s.storage.{LogStorage, SnapshotStorage}
import raft4s.{LogCompactionPolicy, StateMachine}

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future}

private[future] class LogImpl(
  val logStorage: LogStorage[Future],
  val snapshotStorage: SnapshotStorage[Future],
  val stateMachine: StateMachine[Future],
  val membershipManager: MembershipManager[Future],
  val compactionPolicy: LogCompactionPolicy[Future],
  val lastCommit: Long
)(implicit val ME: MonadError[Future, Throwable], val logger: Logger[Future], EC: ExecutionContext)
    extends Log[Future] {

  private val lastCommitIndex = new AtomicLong(lastCommit)
  private val semaphore       = new Semaphore(1)

  override def withPermit[A](code: => Future[A]): Future[A] =
    this.synchronized {
      semaphore.acquire()
      val task = code
      task.onComplete(_ => semaphore.release())
      task
    }

  override def getCommitIndex: Future[Long] =
    Future {
      lastCommitIndex.get()
    }

  override def setCommitIndex(index: Long): Future[Unit] =
    Future {
      lastCommitIndex.set(index)
    }
}

object LogImpl {
  def build(
    logStorage: LogStorage[Future],
    snapshotStorage: SnapshotStorage[Future],
    stateMachine: StateMachine[Future],
    compactionPolicy: LogCompactionPolicy[Future],
    membershipManager: MembershipManager[Future],
    lastCommitIndex: Long
  )(implicit L: Logger[Future], EX: ExecutionContext): raft4s.internal.Log[Future] =
    new LogImpl(
      logStorage,
      snapshotStorage,
      stateMachine,
      membershipManager,
      compactionPolicy,
      lastCommitIndex
    )
}
