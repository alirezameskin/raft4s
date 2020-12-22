package raft4s

case class LogEntry(term: Long, index: Long, command: Command[_])
