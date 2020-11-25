package raft4s.rpc

case class VoteRequest(nodeId: String, currentTerm: Long, logLength: Long, logTerm: Long)
