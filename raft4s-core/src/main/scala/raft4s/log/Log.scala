package raft4s.log

import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Ref, Semaphore}
import cats.implicits._
import cats.{Monad, MonadError}
import io.odin.Logger
import raft4s.{Node, StateMachine}
import raft4s.internal.MembershipManager
import raft4s.protocol._
import raft4s.storage.{LogStorage, Snapshot, SnapshotStorage}

import scala.collection.concurrent.TrieMap

class Log[F[_]: Monad: Logger](
  commitIndexRef: Ref[F, Long],
  lastAppliedRef: Ref[F, Long],
  logStorage: LogStorage[F],
  snapshotStorage: SnapshotStorage[F],
  stateMachine: StateMachine[F],
  membershipManager: MembershipManager[F],
  semaphore: Semaphore[F],
  compactionPolicy: LogCompactionPolicy[F]
)(implicit ME: MonadError[F, Throwable]) {

  private val deferreds = TrieMap[Long, Deferred[F, Any]]()

  def initialize(): F[Unit] =
    semaphore.withPermit {
      for {
        _                 <- Logger[F].trace("Initializing log")
        snapshot          <- snapshotStorage.retrieveSnapshot()
        _                 <- snapshot.map(restoreSnapshot).getOrElse(Monad[F].unit)
        appliedIndex      <- lastAppliedRef.get
        _                 <- Logger[F].trace(s"Latest applied index ${appliedIndex}")
        stateMachineIndex <- stateMachine.appliedIndex
        _                 <- Logger[F].trace(s"State machine applied index ${stateMachineIndex}")
        _ <-
          if (stateMachineIndex > appliedIndex) lastAppliedRef.set(stateMachineIndex)
          else (stateMachineIndex + 1 to appliedIndex).toList.traverse(commit)

      } yield ()
    }

  private def restoreSnapshot(snapshot: Snapshot): F[Unit] =
    for {
      _ <- Logger[F].trace(s"Restoring an Snapshot(index: ${snapshot.lastIndex}), ClusterConfig: ${snapshot.config}")
      _ <- membershipManager.setClusterConfiguration(snapshot.config)
      _ <- stateMachine.restoreSnapshot(snapshot.lastIndex, snapshot.bytes)
      _ <- Logger[F].trace("Snapshot is restored.")
    } yield ()

  def installSnapshot(snapshot: Snapshot, lastEntry: LogEntry): F[Unit] =
    semaphore.withPermit {
      for {
        length <- logStorage.lastIndex
        _ <-
          if (length >= snapshot.lastIndex)
            ME.raiseError(new RuntimeException("A new snapshot is already applied"))
          else Monad[F].unit
        _ <- Logger[F].trace(s"Installing a snapshot, ${snapshot}")
        _ <- snapshotStorage.saveSnapshot(snapshot)
        _ <- Logger[F].trace("Restoring state from snapshot")
        _ <- restoreSnapshot(snapshot)
        _ <- logStorage.put(lastEntry.index, lastEntry)
      } yield ()
    }

  def getAppendEntries(leaderId: Node, term: Long, nextIndex: Long): F[AppendEntries] =
    for {
      lastIndex    <- logStorage.lastIndex
      appliedIndex <- lastAppliedRef.get
      entries      <- (nextIndex to lastIndex).toList.traverse(i => logStorage.get(i))
      prevLogTerm  <- if (nextIndex > 0) logStorage.get(nextIndex - 1).map(_.term) else Monad[F].pure(0L)
    } yield AppendEntries(leaderId, term, nextIndex, prevLogTerm, appliedIndex, entries)

  def state: F[LogState] =
    for {
      length       <- logStorage.lastIndex
      term         <- if (length > 0) logStorage.get(length - 1).map(e => Some(e.term)) else Monad[F].pure(None)
      appliedIndex <- lastAppliedRef.get
    } yield LogState(length, term, appliedIndex)

  def append[T](term: Long, command: Command[T], deferred: Deferred[F, T]): F[LogEntry] =
    for {
      length <- logStorage.lastIndex
      logEntry = LogEntry(term, length, command)
      _ <- Logger[F].trace(s"Appending a command to the log. Term: ${term}, Index: ${length}")
      _ <- logStorage.put(logEntry.index, logEntry)
      _ = deferreds.put(logEntry.index, deferred.asInstanceOf[Deferred[F, Any]])
    } yield logEntry

  def appendEntries(entries: List[LogEntry], leaderLogLength: Long, leaderCommit: Long): F[Boolean] =
    semaphore.withPermit {
      for {
        logLength    <- logStorage.lastIndex
        appliedIndex <- lastAppliedRef.get
        _            <- truncateInconsistentLogs(entries, logLength, leaderLogLength)
        _            <- putEntries(entries, logLength, leaderLogLength)
        committed    <- (appliedIndex + 1 to leaderCommit).toList.traverse(commit)
        _            <- if (committed.nonEmpty) compactLogs() else Monad[F].unit
      } yield committed.nonEmpty
    }

  def commitLogs(ackedLength: Map[Node, Long]): F[Boolean] = {

    def commitIfAcked(index: Long): F[Boolean] =
      for {
        config <- membershipManager.getClusterConfiguration
        acked = config.quorumReached(ackedLength.filter(_._2 >= index).keySet)
        res <- if (acked) commit(index) *> Monad[F].pure(true) else Monad[F].pure(false)
      } yield res

    semaphore.withPermit {
      for {
        logLength    <- logStorage.lastIndex
        appliedIndex <- lastAppliedRef.get
        _            <- Logger[F].trace(s"Applied Index ${appliedIndex}")
        committed    <- (appliedIndex + 1 until logLength).toList.traverse(commitIfAcked)
        _            <- Logger[F].trace(s"Committed ${committed}")
        _            <- if (committed.contains(true)) compactLogs() else Monad[F].unit
      } yield committed.contains(true)
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
          if (entry != null && entry.term != entries.head.term) logStorage.deleteAfter(leaderLogSent)
          else Monad[F].unit
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
      _     <- Logger[F].trace(s"Committing the log entry at index ${index} ${entry}")
      _     <- applyCommand(index, entry.command)
      _     <- lastAppliedRef.set(index)
    } yield ()

  private def applyCommand(index: Long, command: Command[_]): F[Unit] = {
    val output = command match {
      case command: ClusterConfigurationCommand =>
        membershipManager.setClusterConfiguration(command.toConfig)

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

  private def compactLogs() =
    for {
      logState <- this.state
      eligible <- compactionPolicy.eligible(logState, stateMachine)
      _        <- if (eligible) takeSnapshot() else Monad[F].unit
    } yield ()

  private def takeSnapshot(): F[Unit] =
    for {
      _        <- Logger[F].trace("Starting to take snapshot")
      snapshot <- stateMachine.takeSnapshot()
      config   <- membershipManager.getClusterConfiguration
      _        <- snapshotStorage.saveSnapshot(Snapshot(snapshot._1, snapshot._2, config))
      _        <- Logger[F].trace(s"Snapshot is stored ${snapshot}")
      _        <- Logger[F].trace(s"Deleting logs before ${snapshot._1}")
      _        <- logStorage.deleteBefore(snapshot._1)
      _        <- Logger[F].trace(s"Logs before ${snapshot._1} are deleted.")

    } yield ()
}

object Log {
  def build[F[_]: Concurrent: Logger](
    logStorage: LogStorage[F],
    snapshotStorage: SnapshotStorage[F],
    stateMachine: StateMachine[F],
    compactionPolicy: LogCompactionPolicy[F],
    membershipManager: MembershipManager[F],
    appliedIndex: Long
  ): F[Log[F]] =
    for {
      lock           <- Semaphore[F](1)
      lastAppliedRef <- Ref.of[F, Long](0L)
      commitIndexRef <- Ref.of[F, Long](0L)
    } yield new Log(
      commitIndexRef,
      lastAppliedRef,
      logStorage,
      snapshotStorage,
      stateMachine,
      membershipManager,
      lock,
      compactionPolicy
    )
}
