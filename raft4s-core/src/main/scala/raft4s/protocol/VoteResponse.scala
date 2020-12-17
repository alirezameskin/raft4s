package raft4s.protocol

case class VoteResponse(nodeId: Node, term: Long, granted: Boolean)
