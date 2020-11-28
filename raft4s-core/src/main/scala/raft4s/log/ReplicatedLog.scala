package raft4s.log

import cats.implicits._
import cats.effect.IO
import cats.effect.concurrent.Deferred
import raft4s.StateMachine
import raft4s.rpc.{AppendEntries, Command, ReadCommand, WriteCommand}

import scala.collection.concurrent.TrieMap

class ReplicatedLog(log: Log, stateMachine: StateMachine) {

  val deferreds = TrieMap[Long, Deferred[IO, Any]]()

  def getAppendEntries(leaderId: String, term: Long, sentLength: Long): IO[AppendEntries] =
    for {
      length       <- log.length
      commitLength <- log.commitLength
      entries      <- (sentLength until length).toList.traverse(i => log.get(i))
      prevLogTerm = if (sentLength > 0 && entries.nonEmpty) entries.last.term else 0
    } yield AppendEntries(leaderId, term, sentLength, prevLogTerm, commitLength, entries)

  def state: IO[LogState] =
    for {
      length <- log.length
      term   <- if (length > 0) log.get(length - 1).map(e => Some(e.term)) else IO(None)
    } yield LogState(length, term)

  def append[T](term: Long, command: Command[T], deferred: Deferred[IO, T]): IO[LogEntry] =
    for {
      length <- log.length
      logEntry = LogEntry(term, length, command)
      _ <- log.put(logEntry.index, logEntry)
      _ = deferreds.put(logEntry.index, deferred.asInstanceOf[Deferred[IO, Any]])
    } yield logEntry

  def appendEntries(entries: List[LogEntry], leaderLogLength: Long, leaderCommit: Long): IO[Unit] =
    for {
      logLength    <- log.length
      commitLength <- log.commitLength
      _            <- truncateInconsistentLogs(entries, logLength, leaderLogLength)
      _            <- putEntries(entries, logLength, leaderLogLength)
      _            <- (commitLength until leaderCommit).toList.traverse(commit)
    } yield ()

  def commitLogs(ackedLength: Map[String, Long], minAckes: Int): IO[Unit] = {
    val acked: Long => Boolean = index => ackedLength.count(_._2 >= index) >= minAckes

    for {
      logLength    <- log.length
      commitLength <- log.commitLength
      _            <- (commitLength until logLength).filter(i => acked(i + 1)).toList.traverse(commit)
    } yield ()
  }

  private def truncateInconsistentLogs(entries: List[LogEntry], logLength: Long, leaderLogLength: Long): IO[Unit] =
    if (entries.nonEmpty && logLength > leaderLogLength)
      log.get(logLength).flatMap { entry =>
        if (entry.term != entries.head.term) {
          (leaderLogLength until logLength).toList.traverse(log.delete) *> IO.unit
        } else IO.unit
      }
    else IO.unit

  private def putEntries(entries: List[LogEntry], logLength: Long, leaderLogLength: Long): IO[Unit] = {
    val logEntries = if (leaderLogLength + entries.size > logLength) {
      val start = logLength - leaderLogLength
      (start until entries.length).map(i => entries(i.toInt)).toList
    } else List.empty

    logEntries.traverse(entry => log.put(entry.index, entry)) *> IO.unit
  }

  private def commit(index: Long): IO[Unit] =
    for {
      entry <- log.get(index)
      _     <- applyCommand(index, entry.command)
      _     <- log.updateCommitLength(index + 1)
    } yield ()

  private def applyCommand(index: Long, command: Command[_]): IO[Unit] = {
    val result = command match {
      case command: ReadCommand[_] =>
        stateMachine.applyRead.apply(command)

      case command: WriteCommand[_] =>
        stateMachine.applyWrite.apply((index, command))

      case _ =>
        ()
    }

    deferreds.get(index) match {
      case Some(deferred) => deferred.complete(result)
      case None           => IO.unit
    }
  }

}
