package raft4s.log

case class LogState(length: Long, lastTerm: Option[Long], appliedIndex: Option[Long] = None)
