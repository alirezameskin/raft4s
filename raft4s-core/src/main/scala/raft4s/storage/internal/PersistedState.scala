package raft4s.storage.internal

private[raft4s] case class PersistedState(term: Long, votedFor: Option[String])
