package raft4s.protocol

import raft4s.Node

case class VoteRequest(nodeId: Node, currentTerm: Long, logLength: Long, logTerm: Long)
