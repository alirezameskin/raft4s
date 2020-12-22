package raft4s.storage

import raft4s.Node
import raft4s.node.{FollowerNode, NodeState}

case class PersistedState(term: Long, votedFor: Option[Node], appliedIndex: Long = 0L) {
  def toNodeState(nodeId: Node): NodeState =
    FollowerNode(nodeId, term, votedFor = votedFor)
}
