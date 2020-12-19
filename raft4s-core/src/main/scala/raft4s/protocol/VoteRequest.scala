package raft4s.protocol

import raft4s.Node

case class VoteRequest(nodeId: Node, term: Long, lastLogIndex: Long, lastLogTerm: Long)
