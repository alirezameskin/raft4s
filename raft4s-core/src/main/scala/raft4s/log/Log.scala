package raft4s.log

import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Semaphore}
import cats.implicits._
import cats.{Monad, MonadError}
import io.odin.Logger
import raft4s.StateMachine
import raft4s.internal.MembershipManager
import raft4s.protocol._
import raft4s.storage.{LogStorage, Snapshot, SnapshotStorage}

import scala.collection.concurrent.TrieMap

class Log[F[_]: Monad: Logger](
  logStorage: LogStorage[F],
  snapshotStorage: SnapshotStorage[F],
  stateMachine: StateMachine[F],
  membershipManager: MembershipManager[F],
  semaphore: Semaphore[F],
  compactionPolicy: LogCompactionPolicy[F]
)(implicit ME: MonadError[F, Throwable]) {

  private val deferreds = TrieMap[Long, Deferred[F, Any]]()

  def initialize(): F[Unit] =
    for {
      _        <- Logger[F].trace("Initializing log")
      snapshot <- snapshotStorage.retrieveSnapshot()
      _        <- snapshot.map(stateMachine.restoreSnapshot).getOrElse(Monad[F].unit)
      _        <- Logger[F].trace(s"Retrieving snapshot ${snapshot}")
    } yield ()

  def installSnapshot(snapshot: Snapshot, lastEntry: LogEntry): F[Unit] =
    semaphore.withPermit {
      for {
        length <- logStorage.length
        _ <-
          if (length >= snapshot.lastIndex)
            ME.raiseError(new RuntimeException("A new snapshot is already applied"))
          else Monad[F].unit
        _ <- Logger[F].trace(s"Installing a snapshot, ${snapshot}")
        _ <- snapshotStorage.saveSnapshot(snapshot)
        _ <- Logger[F].trace("Restoring state from snapshot")
        _ <- stateMachine.restoreSnapshot(snapshot)
        _ <- Logger[F].trace("Restoring state from snapshot is done")
        _ <- Logger[F].trace(s"Snapshot installation is done ${snapshot}")
        _ <- logStorage.put(lastEntry.index, lastEntry)
      } yield ()
    }

  def getAppendEntries(leaderId: String, term: Long, sentLength: Long): F[AppendEntries] =
    for {
      length       <- logStorage.length
      appliedIndex <- stateMachine.appliedIndex
      entries      <- (sentLength until length).toList.traverse(i => logStorage.get(i))
      prevLogTerm  <- if (sentLength > 0) logStorage.get(sentLength - 1).map(_.term) else Monad[F].pure(0L)
    } yield AppendEntries(leaderId, term, sentLength, prevLogTerm, appliedIndex, entries)

  def state: F[LogState] =
    for {
      length       <- logStorage.length
      term         <- if (length > 0) logStorage.get(length - 1).map(e => Some(e.term)) else Monad[F].pure(None)
      appliedIndex <- stateMachine.appliedIndex
    } yield LogState(length, term, Some(appliedIndex))

  def append[T](term: Long, command: Command[T], deferred: Deferred[F, T]): F[LogEntry] =
    for {
      length <- logStorage.length
      logEntry = LogEntry(term, length, command)
      _ <- Logger[F].trace(s"Appending a command to the log. Term: ${term}, Index: ${length}")
      _ <- logStorage.put(logEntry.index, logEntry)
      _ = deferreds.put(logEntry.index, deferred.asInstanceOf[Deferred[F, Any]])
    } yield logEntry

  def appendEntries(entries: List[LogEntry], leaderLogLength: Long, leaderCommit: Long): F[Unit] =
    semaphore.withPermit {
      for {
        logLength    <- logStorage.length
        appliedIndex <- stateMachine.appliedIndex
        _            <- truncateInconsistentLogs(entries, logLength, leaderLogLength)
        _            <- putEntries(entries, logLength, leaderLogLength)
        committed    <- (appliedIndex + 1 to leaderCommit).toList.traverse(commit)
        _            <- if (committed.nonEmpty) compactLogs() else Monad[F].unit
      } yield ()
    }

  def commitLogs(ackedLength: Map[String, Long], minAckes: Int): F[Unit] = {
    val acked: Long => Boolean = index => ackedLength.count(_._2 >= index) >= minAckes

    semaphore.withPermit {
      for {
        logLength    <- logStorage.length
        appliedIndex <- stateMachine.appliedIndex
        committed    <- (appliedIndex + 1 until logLength).filter(i => acked(i + 1)).toList.traverse(commit)
        _            <- if (committed.nonEmpty) compactLogs() else Monad[F].unit
      } yield ()
    }
  }

  def applyReadCommand[T](command: ReadCommand[T]): F[T] =
    for {
      res <-
        if (stateMachine.applyRead.isDefinedAt(command)) stateMachine.applyRead(command).asInstanceOf[F[T]]
        else ME.raiseError(new RuntimeException("Can not run the command"))
    } yield res

  def get(index: Long): F[LogEntry] =
    logStorage.get(index)

  def latestSnapshot: F[Option[Snapshot]] =
    snapshotStorage.retrieveSnapshot()

  private def truncateInconsistentLogs(entries: List[LogEntry], logLength: Long, leaderLogSent: Long): F[Unit] =
    if (entries.nonEmpty && logLength > leaderLogSent) {
      Logger[F].trace(
        s"Truncating unfinished log entries from the Log. Leader log Length: ${leaderLogSent}, server log length :${logLength} "
      ) *>
        logStorage.get(logLength).flatMap { entry =>
          if (entry != null && entry.term != entries.head.term) {
            (leaderLogSent until logLength).toList.traverse(logStorage.delete) *> Monad[F].unit
          } else Monad[F].unit
        }
    } else Monad[F].unit

  private def putEntries(entries: List[LogEntry], logLength: Long, leaderLogLength: Long): F[Unit] = {
    val logEntries = if (leaderLogLength + entries.size > logLength) {
      val start = logLength - leaderLogLength
      (start until entries.length).map(i => entries(i.toInt)).toList
    } else List.empty

    logEntries.traverse(entry => logStorage.put(entry.index, entry)) *> Monad[F].unit
  }

  private def commit(index: Long): F[Unit] =
    for {
      _     <- Logger[F].trace(s"Committing the log entry at index ${index}")
      entry <- logStorage.get(index)
      _     <- applyCommand(index, entry.command)
    } yield ()

  private def applyCommand(index: Long, command: Command[_]): F[Unit] = {
    val output = command match {
      case command: ClusterConfiguration =>
        membershipManager.changeConfiguration(command)

      case command: ReadCommand[_] =>
        stateMachine.applyRead.apply(command)

      case command: WriteCommand[_] =>
        stateMachine.applyWrite.apply((index, command))
    }

    output.flatMap { result =>
      deferreds.get(index) match {
        case Some(deferred) => deferred.complete(result) *> Monad[F].pure(deferreds.remove(index))
        case None           => Monad[F].unit
      }
    }
  }

  private def takeSnapshot(): F[Unit] =
    for {
      _        <- Logger[F].trace("Starting to take snapshot")
      snapshot <- stateMachine.takeSnapshot()
      _        <- snapshotStorage.saveSnapshot(snapshot)
      _        <- Logger[F].trace(s"Snapshot is stored ${snapshot}")
      _        <- Logger[F].trace(s"Deleting logs before ${snapshot.lastIndex}")
      _        <- logStorage.deleteBefore(snapshot.lastIndex)
      _        <- Logger[F].trace(s"Logs before ${snapshot.lastIndex} are deleted.")

    } yield ()

  private def compactLogs() =
    for {
      logState <- this.state
      eligible <- compactionPolicy.eligible(logState, stateMachine)
      _        <- if (eligible) takeSnapshot() else Monad[F].unit
    } yield ()
}

object Log {
  def build[F[_]: Concurrent: Logger](
    logStorage: LogStorage[F],
    snapshotStorage: SnapshotStorage[F],
    stateMachine: StateMachine[F],
    compactionPolicy: LogCompactionPolicy[F],
    membershipManager: MembershipManager[F]
  ): F[Log[F]] =
    for {
      lock <- Semaphore[F](1)
    } yield new Log(logStorage, snapshotStorage, stateMachine, membershipManager, lock, compactionPolicy)
}
