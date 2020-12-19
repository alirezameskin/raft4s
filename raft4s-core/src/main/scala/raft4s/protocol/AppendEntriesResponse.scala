package raft4s.protocol

import raft4s.Node

case class AppendEntriesResponse(nodeId: Node, currentTerm: Long, ack: Long, success: Boolean)
