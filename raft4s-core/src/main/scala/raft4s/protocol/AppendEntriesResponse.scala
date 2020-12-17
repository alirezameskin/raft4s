package raft4s.protocol

case class AppendEntriesResponse(nodeId: Node, currentTerm: Long, ack: Long, success: Boolean)
