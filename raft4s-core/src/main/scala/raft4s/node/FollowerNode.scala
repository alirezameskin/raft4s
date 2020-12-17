package raft4s.node

import raft4s.log.LogState
import raft4s.protocol._

case class FollowerNode(
                         node: Node,
                         currentTerm: Long,
                         votedFor: Option[Node] = None,
                         currentLeader: Option[Node] = None
) extends NodeState {

  override def onTimer(logState: LogState, config: ClusterConfiguration): (NodeState, List[Action]) = {
    val (state, actions) = CandidateNode(node, currentTerm, logState.lastTerm.getOrElse(0L)).onTimer(logState, config)

    if (state.isInstanceOf[LeaderNode])
      (state, actions)
    else if (this.currentLeader.isDefined)
      (state, ResetLeaderAnnouncer :: actions)
    else
      (state, actions)
  }

  override def onReceive(
    logState: LogState,
    config: ClusterConfiguration,
    msg: VoteRequest
  ): (NodeState, (VoteResponse, List[Action])) = {

    val myLogTerm = logState.lastTerm.getOrElse(0L)
    val logOK     = (msg.logTerm > myLogTerm) || (msg.logTerm == myLogTerm && msg.logLength >= logState.length)
    val termOK =
      (msg.currentTerm > currentTerm) || (msg.currentTerm == currentTerm && (votedFor.isEmpty || votedFor.contains(msg.nodeId)))

    if (logOK && termOK)
      (
        this.copy(currentTerm = msg.currentTerm, votedFor = Some(msg.nodeId)),
        (VoteResponse(node, msg.currentTerm, true), List(StoreState))
      )
    else
      (this, (VoteResponse(node, currentTerm, false), List.empty))
  }

  override def onReceive(logState: LogState, config: ClusterConfiguration, msg: VoteResponse): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReceive(
    logState: LogState,
    config: ClusterConfiguration,
    msg: AppendEntries
  ): (NodeState, (AppendEntriesResponse, List[Action])) = {
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
          AppendEntriesResponse(node, currentTerm_, msg.logLength + msg.entries.length, true),
          stateChangedActions ::: leaderChangeActions
        )
      )
    } else
      (
        this.copy(currentTerm = currentTerm_, votedFor = votedFor_),
        (
          AppendEntriesResponse(node, currentTerm_, logState.appliedIndex.getOrElse(0), false),
          if (currentTerm == currentTerm_) List.empty else List(StoreState)
        )
      )
  }

  override def onReceive(
    logState: LogState,
    cluster: ClusterConfiguration,
    msg: AppendEntriesResponse
  ): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReplicateLog(config: ClusterConfiguration): List[Action] =
    List.empty

  override def leader: Option[Node] =
    currentLeader

  override def toPersistedState: PersistedState =
    PersistedState(currentTerm, votedFor)

  override def onSnapshotInstalled(logState: LogState, config: ClusterConfiguration): (NodeState, AppendEntriesResponse) =
    (this, AppendEntriesResponse(node, currentTerm, logState.length - 1, true))
}
