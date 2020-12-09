package raft4s.log

import cats.Monad
import cats.effect.concurrent.Deferred
import cats.implicits._
import io.odin.Logger
import raft4s.StateMachine
import raft4s.protocol._
import raft4s.storage.LogStorage

import scala.collection.concurrent.TrieMap

class ReplicatedLog[F[_]: Monad: Logger](val storage: LogStorage[F], val stateMachine: StateMachine[F]) {

  private val deferreds = TrieMap[Long, Deferred[F, Any]]()

  def getAppendEntries(leaderId: String, term: Long, sentLength: Long): F[AppendEntries] =
    for {
      length       <- storage.length
      appliedIndex <- stateMachine.appliedIndex
      entries      <- (sentLength until length).toList.traverse(i => storage.get(i))
      prevLogTerm  <- if (sentLength > 0) storage.get(sentLength - 1).map(_.term) else Monad[F].pure(0L)
    } yield AppendEntries(leaderId, term, sentLength, prevLogTerm, appliedIndex, entries)

  def state: F[LogState] =
    for {
      length <- storage.length
      term   <- if (length > 0) storage.get(length - 1).map(e => Some(e.term)) else Monad[F].pure(None)
    } yield LogState(length, term)

  def append[T](term: Long, command: Command[T], deferred: Deferred[F, T]): F[LogEntry] =
    for {
      length <- storage.length
      logEntry = LogEntry(term, length, command)
      _ <- Logger[F].trace(s"Appending a command to the log. Term: ${term}, Index: ${length}")
      _ <- storage.put(logEntry.index, logEntry)
      _ = deferreds.put(logEntry.index, deferred.asInstanceOf[Deferred[F, Any]])
    } yield logEntry

  def appendEntries(entries: List[LogEntry], leaderLogLength: Long, leaderCommit: Long): F[Unit] =
    for {
      logLength    <- storage.length
      appliedIndex <- stateMachine.appliedIndex
      _            <- truncateInconsistentLogs(entries, logLength, leaderLogLength)
      _            <- putEntries(entries, logLength, leaderLogLength)
      _            <- (appliedIndex + 1 to leaderCommit).toList.traverse(commit)
    } yield ()

  def commitLogs(ackedLength: Map[String, Long], minAckes: Int): F[Unit] = {
    val acked: Long => Boolean = index => ackedLength.count(_._2 >= index) >= minAckes

    for {
      logLength    <- storage.length
      appliedIndex <- stateMachine.appliedIndex
      _            <- (appliedIndex + 1 until logLength).filter(i => acked(i + 1)).toList.traverse(commit)
    } yield ()
  }

  private def truncateInconsistentLogs(entries: List[LogEntry], logLength: Long, leaderLogSent: Long): F[Unit] =
    if (entries.nonEmpty && logLength > leaderLogSent) {
      Logger[F].trace(
        s"Truncating unfinished log entries from the Log. Leader log Length: ${leaderLogSent}, server log length :${logLength} "
      ) *>
        storage.get(logLength).flatMap { entry =>
          if (entry != null && entry.term != entries.head.term) {
            (leaderLogSent until logLength).toList.traverse(storage.delete) *> Monad[F].unit
          } else Monad[F].unit
        }
    } else Monad[F].unit

  private def putEntries(entries: List[LogEntry], logLength: Long, leaderLogLength: Long): F[Unit] = {
    val logEntries = if (leaderLogLength + entries.size > logLength) {
      val start = logLength - leaderLogLength
      (start until entries.length).map(i => entries(i.toInt)).toList
    } else List.empty

    logEntries.traverse(entry => storage.put(entry.index, entry)) *> Monad[F].unit
  }

  private def commit(index: Long): F[Unit] =
    for {
      _     <- Logger[F].trace(s"Committing the log entry at index ${index}")
      entry <- storage.get(index)
      _     <- applyCommand(index, entry.command)
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

}

object ReplicatedLog {
  def build[F[_]: Monad: Logger](storage: LogStorage[F], stateMachine: StateMachine[F]): ReplicatedLog[F] =
    new ReplicatedLog(storage, stateMachine)
}
