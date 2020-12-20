package raft4s.protocol

case class LogState(lastLogIndex: Long, lastLogTerm: Option[Long], lastAppliedIndex: Long = 0)
