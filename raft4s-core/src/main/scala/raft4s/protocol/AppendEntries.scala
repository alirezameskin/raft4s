package raft4s.protocol

import raft4s.Node

case class AppendEntries(
  leaderId: Node,
  term: Long,
  logLength: Long,
  logTerm: Long,
  leaderAppliedIndex: Long,
  entries: List[LogEntry]
)
