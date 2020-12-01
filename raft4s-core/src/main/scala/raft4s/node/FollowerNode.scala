package raft4s.node

import raft4s._
import raft4s.log.LogState
import raft4s.rpc._

case class FollowerNode(
  nodeId: String,
  nodes: List[String],
  currentTerm: Long,
  votedFor: Option[String] = None,
  currentLeader: Option[String] = None
) extends NodeState {

  override def onTimer(logState: LogState): (NodeState, List[Action]) =
    CandidateNode(nodeId, nodes, currentTerm, logState.lastTerm.getOrElse(0L)).onTimer(logState)

  override def onReceive(logState: LogState, msg: VoteRequest): (NodeState, VoteResponse) = {

    val myLogTerm = logState.lastTerm.getOrElse(0L)
    val logOK     = (msg.logTerm > myLogTerm) || (msg.logTerm == myLogTerm && msg.logLength >= logState.length)
    val termOK =
      (msg.currentTerm > currentTerm) || (msg.currentTerm == currentTerm && (votedFor.isEmpty || votedFor.contains(msg.nodeId)))

    if (logOK && termOK)
      (this.copy(currentTerm = msg.currentTerm, votedFor = Some(msg.nodeId)), VoteResponse(nodeId, msg.currentTerm, true))
    else
      (this, VoteResponse(nodeId, currentTerm, false))
  }

  override def onReceive(logState: LogState, msg: VoteResponse): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReceive(logState: LogState, msg: AppendEntries): (NodeState, AppendEntriesResponse) = {
    val currentTerm_ = if (msg.term > currentTerm) msg.term else currentTerm
    val votedFor_    = if (msg.term > currentTerm) None else votedFor

    val logOK_ = logState.length >= msg.logLength
    val logOK = if (logOK_ && msg.logLength > 0) { logState.lastTerm.contains(msg.logTerm) }
    else logOK_

    if (msg.term == currentTerm_ && logOK)
      (
        this.copy(currentTerm = currentTerm_, votedFor = votedFor_, currentLeader = Some(msg.leaderId)),
        AppendEntriesResponse(nodeId, currentTerm_, msg.logLength + msg.entries.length, true)
      )
    else
      (this.copy(currentTerm = currentTerm_, votedFor = votedFor_), AppendEntriesResponse(nodeId, currentTerm_, 0, false))
  }

  override def onReceive(logState: LogState, msg: AppendEntriesResponse): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReplicateLog(): List[Action] =
    List.empty
}
