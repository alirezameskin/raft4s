package raft4s.protocol

private[raft4s] case class PersistedState(term: Long, votedFor: Option[String])
