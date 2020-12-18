package raft4s.protocol

import raft4s.Node

private[raft4s] case class PersistedState(term: Long, votedFor: Option[Node], appliedIndex: Long = -1L)
