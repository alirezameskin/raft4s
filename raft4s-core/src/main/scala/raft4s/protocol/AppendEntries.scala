package raft4s.protocol

import raft4s.Node

case class AppendEntries(
  leaderId: Node,
  term: Long,
  prevLogIndex: Long,
  prevLogTerm: Long,
  leaderCommit: Long,
  entries: List[LogEntry]
)
