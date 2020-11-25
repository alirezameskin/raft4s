package raft4s.log

import cats.effect.IO

trait Log {
  def length: IO[Long]

  def commitLength: IO[Long]

  def updateCommitLength(index: Long): IO[Unit]

  def get(index: Long): IO[LogEntry]

  def put(index: Long, logEntry: LogEntry): IO[LogEntry]
}
