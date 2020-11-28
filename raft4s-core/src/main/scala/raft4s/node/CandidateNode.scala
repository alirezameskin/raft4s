package raft4s.node

import raft4s.log.LogState
import raft4s.rpc._
import raft4s.{Action, ReplicateLog, RequestForVote, StartElectionTimer}

case class CandidateNode(
  nodeId: String,
  nodes: List[String],
  currentTerm: Long,
  lastTerm: Long,
  votedFor: Option[String],
  votedReceived: Set[String]
) extends NodeState {

  override def onTimer(logState: LogState): (NodeState, List[Action]) = {

    val lastTerm_ = logState.lastTerm.getOrElse(lastTerm)
    val request   = VoteRequest(nodeId, currentTerm, logState.length, lastTerm_)
    val actions   = nodes.map(nodeId => RequestForVote(nodeId, request))

    (this.copy(lastTerm = lastTerm_), StartElectionTimer :: actions)
  }

  override def onReceive(logState: LogState, msg: VoteRequest): (NodeState, VoteResponse) = {

    val myLogTerm = logState.lastTerm.getOrElse(0L)
    val logOK     = (msg.logTerm > myLogTerm) || (msg.logTerm == myLogTerm && msg.logLength >= logState.length)
    val termOK =
      (msg.currentTerm > currentTerm) || (msg.currentTerm == currentTerm && (votedFor.isEmpty || votedFor.contains(msg.nodeId)))

    if (logOK && termOK) {
      (
        this.copy(currentTerm = msg.currentTerm, votedFor = Some(msg.nodeId)),
        VoteResponse(nodeId, currentTerm, true)
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
    val votedFor_    = None

    val logOK_ = msg.logLength >= logState.length
    val logOK = if (logOK_ && msg.logLength > 0) { logState.lastTerm.contains(msg.logTerm) }
    else logOK_

    if (msg.term == currentTerm && logOK)
      (
        FollowerNode(nodeId, nodes, currentTerm_),
        AppendEntriesResponse(nodeId, currentTerm_, msg.logLength + msg.entries.length, true)
      )
    else
      (
        this.copy(currentTerm = currentTerm_, votedFor = votedFor_),
        AppendEntriesResponse(nodeId, currentTerm_, 0, false)
      )
  }

  override def onReceive(logState: LogState, msg: AppendEntriesResponse): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReplicateLog(): List[Action] =
    List.empty
}
