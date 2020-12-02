package raft4s.protocol

case class AppendEntriesResponse(nodeId: String, currentTerm: Long, ack: Long, success: Boolean)
