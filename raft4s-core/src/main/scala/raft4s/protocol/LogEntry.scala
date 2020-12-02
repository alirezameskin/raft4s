package raft4s.protocol

case class LogEntry(term: Long, index: Long, command: Command[_])
