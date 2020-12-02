package raft4s.protocol

case class VoteRequest(nodeId: String, currentTerm: Long, logLength: Long, logTerm: Long)
