package raft4s.log

import raft4s.rpc.Command

case class LogEntry(term: Long, index: Long, command: Command[_])
