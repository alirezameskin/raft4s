package raft4s.rpc

import raft4s.log.LogEntry

case class AppendEntries(
  leaderId: String,
  term: Long,
  logLength: Long,
  logTerm: Long,
  leaderCommit: Long,
  entries: List[LogEntry]
)
