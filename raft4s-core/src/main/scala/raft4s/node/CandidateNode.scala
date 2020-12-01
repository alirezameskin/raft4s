package raft4s.node

import raft4s.log.LogState
import raft4s.rpc._
import raft4s.{Action, ReplicateLog, RequestForVote}

case class CandidateNode(
  nodeId: String,
  nodes: List[String],
  currentTerm: Long,
  lastTerm: Long,
  votedFor: Option[String] = None,
  votedReceived: Set[String] = Set.empty
) extends NodeState {

  override def onTimer(logState: LogState): (NodeState, List[Action]) = {

    val currentTerm_ = currentTerm + 1
    val lastTerm_    = logState.lastTerm.getOrElse(lastTerm)
    val request      = VoteRequest(nodeId, currentTerm_, logState.length, lastTerm_)
    val actions      = nodes.filterNot(_ == nodeId).map(nodeId => RequestForVote(nodeId, request))

    (
      this.copy(currentTerm = currentTerm_, lastTerm = lastTerm_, votedFor = Some(nodeId), votedReceived = Set(nodeId)),
      actions
    )
  }

  override def onReceive(logState: LogState, msg: VoteRequest): (NodeState, VoteResponse) = {

    val myLogTerm = logState.lastTerm.getOrElse(0L)
    val logOK     = (msg.logTerm > myLogTerm) || (msg.logTerm == myLogTerm && msg.logLength >= logState.length)
    val termOK =
      (msg.currentTerm > currentTerm) || (msg.currentTerm == currentTerm && (votedFor.isEmpty || votedFor.contains(msg.nodeId)))

    if (logOK && termOK) {
      (
        FollowerNode(nodeId, nodes, msg.currentTerm, Some(msg.nodeId), None),
        VoteResponse(nodeId, msg.currentTerm, true)
      )
    } else {
      (this, VoteResponse(nodeId, currentTerm, false))
    }
  }

  override def onReceive(logState: LogState, msg: VoteResponse): (NodeState, List[Action]) = {
    val votedReceived_ = votedReceived + msg.nodeId
    val quorumSize     = (nodes.length + 1) / 2

    if (msg.term > currentTerm)
      (FollowerNode(nodeId, nodes, msg.term), List.empty)
    else if (msg.term == currentTerm && msg.granted && votedReceived_.size >= quorumSize) {
      val ackedLength = nodes.filterNot(_ == nodeId).map(n => (n, logState.length)).toMap
      val sentLength  = nodes.filterNot(_ == nodeId).map(n => (n, 0L)).toMap
      val actions     = nodes.filterNot(_ == nodeId).map(n => ReplicateLog(n, currentTerm, 0))

      (LeaderNode(nodeId, nodes, currentTerm, ackedLength, sentLength), actions)

    } else
      (this.copy(votedReceived = votedReceived_), List.empty)
  }

  override def onReceive(logState: LogState, msg: AppendEntries): (NodeState, AppendEntriesResponse) = {

    val currentTerm_ = if (msg.term > currentTerm) msg.term else currentTerm

    val logOK_ = msg.logLength >= logState.length
    val logOK = if (logOK_ && msg.logLength > 0) { logState.lastTerm.contains(msg.logTerm) }
    else logOK_

    if (msg.term == currentTerm_ && logOK)
      (
        FollowerNode(nodeId, nodes, currentTerm_, None, Some(msg.leaderId)),
        AppendEntriesResponse(nodeId, currentTerm_, msg.logLength + msg.entries.length, true)
      )
    else
      (
        this.copy(currentTerm = currentTerm_),
        AppendEntriesResponse(nodeId, currentTerm_, 0, false)
      )
  }

  override def onReceive(logState: LogState, msg: AppendEntriesResponse): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReplicateLog(): List[Action] =
    List.empty
}
