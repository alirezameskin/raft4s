package raft4s.rpc

case class AppendEntriesResponse(nodeId: String, currentTerm: Long, ack: Long, success: Boolean)
