package raft4s.protocol

import raft4s.Node

case class VoteResponse(nodeId: Node, term: Long, voteGranted: Boolean)
