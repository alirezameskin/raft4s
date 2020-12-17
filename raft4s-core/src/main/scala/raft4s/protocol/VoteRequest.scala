package raft4s.protocol

case class VoteRequest(nodeId: Node, currentTerm: Long, logLength: Long, logTerm: Long)
