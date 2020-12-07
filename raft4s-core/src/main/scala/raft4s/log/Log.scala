package raft4s.log

import raft4s.protocol.LogEntry

trait Log[F[_]] {
  def length: F[Long]

  def get(index: Long): F[LogEntry]

  def put(index: Long, logEntry: LogEntry): F[LogEntry]

  def delete(index: Long): F[Unit]
}
