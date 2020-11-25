package raft4s.rpc

case class VoteResponse(nodeId: String, term: Long, granted: Boolean)
