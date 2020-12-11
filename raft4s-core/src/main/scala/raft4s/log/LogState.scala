package raft4s.log

import raft4s.storage.Snapshot

case class LogState(length: Long, lastTerm: Option[Long], appliedIndex: Option[Long] = None, snapshot: Option[Snapshot] = None)
