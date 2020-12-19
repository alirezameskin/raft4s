package raft4s.log

case class LogState(lastIndex: Long, lastTerm: Option[Long], lastApplied: Long = 0)
