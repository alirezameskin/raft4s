package raft4s.log

import cats.Monad
import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Semaphore}
import cats.implicits._
import io.odin.Logger
import raft4s.StateMachine
import raft4s.protocol._
import raft4s.storage.{LogStorage, SnapshotStorage}

import scala.collection.concurrent.TrieMap

class ReplicatedLog[F[_]: Monad: Logger](
  val logStorage: LogStorage[F],
  val snapshotStorage: SnapshotStorage[F],
  val stateMachine: StateMachine[F],
  val semaphore: Semaphore[F]
) {

  private val deferreds = TrieMap[Long, Deferred[F, Any]]()

  def initialize(): F[Unit] =
    for {
      _        <- Logger[F].trace("Initializing log")
      snapshot <- snapshotStorage.retrieveSnapshot()
      _        <- snapshot.map(stateMachine.restoreSnapshot).getOrElse(Monad[F].unit)
      _        <- Logger[F].trace(s"Retrieving snapshot ${snapshot}")
    } yield ()

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
    for {
      _            <- semaphore.acquire
      logLength    <- logStorage.length
      appliedIndex <- stateMachine.appliedIndex
      _            <- truncateInconsistentLogs(entries, logLength, leaderLogLength)
      _            <- putEntries(entries, logLength, leaderLogLength)
      _            <- (appliedIndex + 1 to leaderCommit).toList.traverse(commit)
      _            <- semaphore.release
    } yield ()

  def commitLogs(ackedLength: Map[String, Long], minAckes: Int): F[Unit] = {
    val acked: Long => Boolean = index => ackedLength.count(_._2 >= index) >= minAckes

    for {
      logLength    <- logStorage.length
      appliedIndex <- stateMachine.appliedIndex
      _            <- (appliedIndex + 1 until logLength).filter(i => acked(i + 1)).toList.traverse(commit)
    } yield ()
  }

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
      _     <- if (index % 5 == 0) takeSnapshot() else Monad[F].unit
    } yield ()

  private def applyCommand(index: Long, command: Command[_]): F[Unit] = {
    val output = command match {
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
      _        <- Logger[F].trace("Snapshot is stored")
    } yield ()

}

object ReplicatedLog {
  def build[F[_]: Concurrent: Logger](
    logStorage: LogStorage[F],
    snapshotStorage: SnapshotStorage[F],
    stateMachine: StateMachine[F]
  ): F[ReplicatedLog[F]] =
    for {
      lock <- Semaphore[F](1)
    } yield new ReplicatedLog(logStorage, snapshotStorage, stateMachine, lock)
}
