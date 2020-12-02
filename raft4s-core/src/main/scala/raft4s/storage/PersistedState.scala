package raft4s.storage

case class PersistedState(term: Long, votedFor: Option[String])
