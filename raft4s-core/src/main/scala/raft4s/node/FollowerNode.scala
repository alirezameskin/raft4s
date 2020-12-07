package raft4s.node

import raft4s._
import raft4s.log.LogState
import raft4s.protocol.{AppendEntries, AppendEntriesResponse, VoteRequest, VoteResponse}
import raft4s.storage.PersistedState

case class FollowerNode(
  nodeId: String,
  nodes: List[String],
  currentTerm: Long,
  votedFor: Option[String] = None,
  currentLeader: Option[String] = None
) extends NodeState {

  override def onTimer(logState: LogState): (NodeState, List[Action]) = {
    val (state, actions) = CandidateNode(nodeId, nodes, currentTerm, logState.lastTerm.getOrElse(0L)).onTimer(logState)

    if (state.isInstanceOf[LeaderNode])
      (state, actions)
    else if (this.currentLeader.isDefined)
      (state, ResetLeaderAnnouncer :: actions)
    else
      (state, actions)
  }

  override def onReceive(logState: LogState, msg: VoteRequest): (NodeState, (VoteResponse, List[Action])) = {

    val myLogTerm = logState.lastTerm.getOrElse(0L)
    val logOK     = (msg.logTerm > myLogTerm) || (msg.logTerm == myLogTerm && msg.logLength >= logState.length)
    val termOK =
      (msg.currentTerm > currentTerm) || (msg.currentTerm == currentTerm && (votedFor.isEmpty || votedFor.contains(msg.nodeId)))

    if (logOK && termOK)
      (
        this.copy(currentTerm = msg.currentTerm, votedFor = Some(msg.nodeId)),
        (VoteResponse(nodeId, msg.currentTerm, true), List(StoreState))
      )
    else
      (this, (VoteResponse(nodeId, currentTerm, false), List.empty))
  }

  override def onReceive(logState: LogState, msg: VoteResponse): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReceive(logState: LogState, msg: AppendEntries): (NodeState, (AppendEntriesResponse, List[Action])) = {
    val currentTerm_ = if (msg.term > currentTerm) msg.term else currentTerm
    val votedFor_    = if (msg.term > currentTerm) None else votedFor

    val logOK_ = logState.length >= msg.logLength
    val logOK = if (logOK_ && msg.logLength > 0) { logState.lastTerm.contains(msg.logTerm) }
    else logOK_

    if (msg.term == currentTerm_ && logOK) {

      val leaderChangeActions =
        if (currentLeader.isEmpty)
          List(AnnounceLeader(msg.leaderId))
        else if (currentLeader.contains(msg.leaderId))
          List.empty
        else
          List(AnnounceLeader(msg.leaderId, true))

      val stateChangedActions = if (currentTerm != currentTerm_) List(StoreState) else List.empty

      (
        this.copy(currentTerm = currentTerm_, votedFor = votedFor_, currentLeader = Some(msg.leaderId)),
        (
          AppendEntriesResponse(nodeId, currentTerm_, msg.logLength + msg.entries.length, true),
          stateChangedActions ::: leaderChangeActions
        )
      )
    } else
      (
        this.copy(currentTerm = currentTerm_, votedFor = votedFor_),
        (AppendEntriesResponse(nodeId, currentTerm_, 0, false), if (currentTerm == currentTerm_) List.empty else List(StoreState))
      )
  }

  override def onReceive(logState: LogState, msg: AppendEntriesResponse): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReplicateLog(): List[Action] =
    List.empty

  override def leader: Option[String] =
    currentLeader

  override def toPersistedState: PersistedState =
    PersistedState(currentTerm, votedFor)
}
