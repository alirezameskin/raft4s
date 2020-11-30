package raft4s.log

trait Log[F[_]] {
  def length: F[Long]

  def commitLength: F[Long]

  def updateCommitLength(index: Long): F[Unit]

  def get(index: Long): F[LogEntry]

  def put(index: Long, logEntry: LogEntry): F[LogEntry]

  def delete(index: Long): F[Unit]
}
