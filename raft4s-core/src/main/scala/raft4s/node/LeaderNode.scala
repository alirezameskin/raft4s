package raft4s.node

import raft4s.log.LogState
import raft4s.rpc._
import raft4s.{Action, CommitLogs, ReplicateLog}

case class LeaderNode(
  nodeId: String,
  nodes: List[String],
  currentTerm: Long,
  ackedLength: Map[String, Long],
  sentLength: Map[String, Long]
) extends NodeState {

  override def onTimer(logState: LogState): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReceive(logState: LogState, msg: VoteRequest): (NodeState, VoteResponse) =
    if (msg.currentTerm > currentTerm)
      ???
    else
      (this, VoteResponse(nodeId, msg.currentTerm, false))

  override def onReceive(logState: LogState, msg: VoteResponse): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReceive(logState: LogState, msg: AppendEntries): (NodeState, AppendEntriesResponse) =
    (this, AppendEntriesResponse(nodeId, currentTerm, 0, false)) //Double check

  override def onReceive(logState: LogState, msg: AppendEntriesResponse): (NodeState, List[Action]) =
    if (msg.currentTerm == currentTerm)
      if (msg.success && msg.ack > ackedLength(msg.nodeId)) {
        val sentLength_  = sentLength + (msg.nodeId  -> msg.ack)
        val ackedLength_ = ackedLength + (msg.nodeId -> msg.ack)

        (this.copy(ackedLength = ackedLength_, sentLength = sentLength_), List(CommitLogs(ackedLength_, (nodes.size + 1) / 2)))

      } else if (sentLength(msg.nodeId) > 0) {
        val sentLength_ = sentLength + (msg.nodeId -> (sentLength(msg.nodeId) - 1))
        val actions = nodes.filterNot(_ == nodeId).map { peer =>
          ReplicateLog(peer, currentTerm, sentLength_(peer))
        }

        (this.copy(sentLength = sentLength_), actions)
      } else
        (this, List.empty)
    else if (msg.currentTerm > currentTerm)
      (FollowerNode(nodeId, nodes, currentTerm), List.empty)
    else
      (this, List.empty)

  override def onReplicateLog(): List[Action] =
    nodes.filterNot(_ == nodeId).map { peer =>
      ReplicateLog(peer, currentTerm, sentLength(peer))
    }
}
