package raft4s.protocol

case class VoteResponse(nodeId: String, term: Long, granted: Boolean)
