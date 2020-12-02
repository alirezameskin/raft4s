package raft4s.protocol

case class AppendEntries(
  leaderId: String,
  term: Long,
  logLength: Long,
  logTerm: Long,
  leaderCommit: Long,
  entries: List[LogEntry]
)