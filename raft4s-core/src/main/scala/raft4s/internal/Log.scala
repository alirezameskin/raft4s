package raft4s.internal

import cats.implicits._
import cats.{Monad, MonadError}
import raft4s.protocol._
import raft4s.storage.{LogStorage, Snapshot, SnapshotStorage}
import raft4s.{LogCompactionPolicy, Node, StateMachine}

import scala.collection.concurrent.TrieMap

abstract class Log[F[_]] {

  implicit val logger: Logger[F]
  implicit val ME: MonadError[F, Throwable]

  private val deferreds = TrieMap[Long, Deferred[F, Any]]()

  val logStorage: LogStorage[F]

  val snapshotStorage: SnapshotStorage[F]

  val stateMachine: StateMachine[F]

  val membershipManager: MembershipManager[F]

  val compactionPolicy: LogCompactionPolicy[F]

  def withPermit[A](t: F[A]): F[A]

  def getCommitIndex: F[Long]

  def setCommitIndex(index: Long): F[Unit]

  def initialize(): F[Unit] =
    withPermit {
      for {
        _                 <- logger.trace("Initializing log")
        snapshot          <- snapshotStorage.retrieveSnapshot()
        _                 <- snapshot.map(restoreSnapshot).getOrElse(Monad[F].unit)
        commitIndex       <- getCommitIndex
        _                 <- logger.trace(s"Latest committed index ${commitIndex}")
        stateMachineIndex <- stateMachine.appliedIndex
        _                 <- logger.trace(s"State machine applied index ${stateMachineIndex}")
        _ <-
          if (stateMachineIndex > commitIndex) setCommitIndex(stateMachineIndex)
          else (stateMachineIndex + 1 to commitIndex).toList.traverse(commit)

      } yield ()
    }

  def state: F[LogState] =
    for {
      lastIndex   <- logStorage.lastIndex
      lastTerm    <- if (lastIndex > 0) logStorage.get(lastIndex).map(e => Some(e.term)) else Monad[F].pure(None)
      commitIndex <- getCommitIndex
    } yield LogState(lastIndex, lastTerm, commitIndex)

  def get(index: Long): F[LogEntry] =
    logStorage.get(index)

  def applyReadCommand[T](command: ReadCommand[T]): F[T] =
    for {
      res <-
        if (stateMachine.applyRead.isDefinedAt(command)) stateMachine.applyRead(command).asInstanceOf[F[T]]
        else ME.raiseError(new RuntimeException("Could not run the command"))
    } yield res

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

  def getAppendEntries(leaderId: Node, term: Long, nextIndex: Long): F[AppendEntries] =
    for {
      _           <- logger.trace(s"Getting append entries since ${nextIndex}")
      lastIndex   <- logStorage.lastIndex
      lastEntry   <- if (nextIndex > 1) logStorage.get(nextIndex - 1).map(Option(_)) else Monad[F].pure(None)
      commitIndex <- getCommitIndex
      entries     <- (nextIndex to lastIndex).toList.traverse(i => logStorage.get(i))
      prevLogIndex = lastEntry.map(_.index).getOrElse(0L)
      prevLogTerm  = lastEntry.map(_.term).getOrElse(0L)
    } yield AppendEntries(leaderId, term, prevLogIndex, prevLogTerm, commitIndex, entries)

  def append[T](term: Long, command: Command[T], deferred: Deferred[F, T]): F[LogEntry] =
    for {
      lastIndex <- logStorage.lastIndex
      logEntry = LogEntry(term, lastIndex + 1, command)
      _ <- logger.trace(s"Appending a command to the log. Term: ${term}, Index: ${lastIndex + 1}")
      _ <- logStorage.put(logEntry.index, logEntry)
      _ = deferreds.put(logEntry.index, deferred.asInstanceOf[Deferred[F, Any]])
    } yield logEntry

  def appendEntries(entries: List[LogEntry], leaderPrevLogIndex: Long, leaderCommit: Long): F[Boolean] =
    withPermit {
      for {
        lastIndex    <- logStorage.lastIndex
        appliedIndex <- getCommitIndex
        _            <- truncateInconsistentLogs(entries, leaderPrevLogIndex, lastIndex)
        _            <- putEntries(entries, leaderPrevLogIndex, lastIndex)
        committed    <- (appliedIndex + 1 to leaderCommit).toList.traverse(commit)
        _            <- if (committed.nonEmpty) compactLogs() else Monad[F].unit
      } yield committed.nonEmpty
    }

  private def truncateInconsistentLogs(entries: List[LogEntry], leaderLogSent: Long, lastLogIndex: Long): F[Unit] =
    if (entries.nonEmpty && lastLogIndex > leaderLogSent) {
      logger.trace(
        s"Truncating log entries from the Log after: ${leaderLogSent}"
      ) *>
        logStorage.get(lastLogIndex).flatMap { entry =>
          if (entry != null && entry.term != entries.head.term) logStorage.deleteAfter(leaderLogSent)
          else Monad[F].unit
        }
    } else Monad[F].unit

  private def putEntries(entries: List[LogEntry], leaderPrevLogIndex: Long, logLastIndex: Long): F[Unit] = {
    val logEntries = if (leaderPrevLogIndex + entries.size > logLastIndex) {
      val start = logLastIndex - leaderPrevLogIndex
      (start until entries.length).map(i => entries(i.toInt)).toList
    } else List.empty

    logEntries.traverse(entry => logStorage.put(entry.index, entry)) *> Monad[F].unit
  }

  def commitLogs(matchIndex: Map[Node, Long]): F[Boolean] =
    withPermit {
      for {
        lastIndex   <- logStorage.lastIndex
        commitIndex <- getCommitIndex
        committed   <- (commitIndex + 1 to lastIndex).toList.traverse(commitIfMatched(matchIndex, _))
        _           <- if (committed.contains(true)) compactLogs() else Monad[F].unit
      } yield committed.contains(true)
    }

  private def commitIfMatched(matchIndex: Map[Node, Long], index: Long): F[Boolean] =
    for {
      config <- membershipManager.getClusterConfiguration
      matched = config.quorumReached(matchIndex.filter(_._2 >= index).keySet)
      result <- if (matched) commit(index) *> Monad[F].pure(true) else Monad[F].pure(false)
    } yield result

  private def commit(index: Long): F[Unit] =
    for {
      _     <- logger.trace(s"Committing the log entry at index ${index}")
      entry <- logStorage.get(index)
      _     <- logger.trace(s"Committing the log entry at index ${index} ${entry}")
      _     <- applyCommand(index, entry.command)
      _     <- setCommitIndex(index)
    } yield ()

  def latestSnapshot: F[Option[Snapshot]] =
    snapshotStorage.retrieveSnapshot()

  def installSnapshot(snapshot: Snapshot, lastEntry: LogEntry): F[Unit] =
    withPermit {
      for {
        lastIndex <- logStorage.lastIndex
        _ <-
          if (lastIndex >= snapshot.lastIndex)
            ME.raiseError(new RuntimeException("A new snapshot is already applied"))
          else Monad[F].unit
        _ <- logger.trace(s"Installing a snapshot, ${snapshot}")
        _ <- snapshotStorage.saveSnapshot(snapshot)
        _ <- logger.trace("Restoring state from snapshot")
        _ <- restoreSnapshot(snapshot)
        _ <- logStorage.put(lastEntry.index, lastEntry)
        _ <- setCommitIndex(snapshot.lastIndex)
      } yield ()
    }

  private def compactLogs(): F[Unit] =
    for {
      logState <- this.state
      eligible <- compactionPolicy.eligible(logState, stateMachine)
      _        <- if (eligible) takeSnapshot() else Monad[F].unit
    } yield ()

  private def takeSnapshot(): F[Unit] =
    for {
      _        <- logger.trace("Starting to take snapshot")
      snapshot <- stateMachine.takeSnapshot()
      config   <- membershipManager.getClusterConfiguration
      _        <- snapshotStorage.saveSnapshot(Snapshot(snapshot._1, snapshot._2, config))
      _        <- logger.trace(s"Snapshot is stored ${snapshot}")
      _        <- logger.trace(s"Deleting logs before ${snapshot._1}")
      _        <- logStorage.deleteBefore(snapshot._1)
      _        <- logger.trace(s"Logs before ${snapshot._1} are deleted.")

    } yield ()

  private def restoreSnapshot(snapshot: Snapshot): F[Unit] =
    for {
      _ <- logger.trace(s"Restoring an Snapshot(index: ${snapshot.lastIndex}), ClusterConfig: ${snapshot.config}")
      _ <- membershipManager.setClusterConfiguration(snapshot.config)
      _ <- stateMachine.restoreSnapshot(snapshot.lastIndex, snapshot.bytes)
      _ <- logger.trace("Snapshot is restored.")
    } yield ()
}
