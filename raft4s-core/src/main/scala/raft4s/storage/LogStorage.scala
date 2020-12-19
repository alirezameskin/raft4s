package raft4s.storage

import raft4s.protocol.LogEntry

trait LogStorage[F[_]] {

  def lastIndex: F[Long]

  def get(index: Long): F[LogEntry]

  def put(index: Long, logEntry: LogEntry): F[LogEntry]

  def deleteBefore(index: Long): F[Unit]

  def deleteAfter(index: Long): F[Unit]
}
