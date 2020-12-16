package raft4s.node

import raft4s.log.LogState
import raft4s.protocol.{PersistedState, _}

case class CandidateNode(
  nodeId: String,
  currentTerm: Long,
  lastTerm: Long,
  votedFor: Option[String] = None,
  votedReceived: Set[String] = Set.empty
) extends NodeState {

  override def onTimer(logState: LogState, config: ClusterConfiguration): (NodeState, List[Action]) = {
    val currentTerm_ = currentTerm + 1
    val lastTerm_    = logState.lastTerm.getOrElse(lastTerm)
    val request      = VoteRequest(nodeId, currentTerm_, logState.length, lastTerm_)
    val actions      = config.members.toList.filterNot(_ == nodeId).map(nodeId => RequestForVote(nodeId, request))
    val quorumSize   = (config.members.size + 1) / 2

    if (1 >= quorumSize) {
      val ackedLength = config.members.filterNot(_ == nodeId).map(n => (n, logState.length)).toMap
      val sentLength  = config.members.filterNot(_ == nodeId).map(n => (n, logState.length)).toMap
      val actions     = config.members.filterNot(_ == nodeId).map(n => ReplicateLog(n, currentTerm, logState.length)).toList

      (LeaderNode(nodeId, currentTerm, ackedLength, sentLength), StoreState :: AnnounceLeader(nodeId) :: actions)
    } else {
      (
        this.copy(currentTerm = currentTerm_, lastTerm = lastTerm_, votedFor = Some(nodeId), votedReceived = Set(nodeId)),
        StoreState :: actions
      )
    }
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

    if (logOK && termOK) {
      (
        FollowerNode(nodeId, msg.currentTerm, Some(msg.nodeId), None),
        (VoteResponse(nodeId, msg.currentTerm, true), List(StoreState))
      )
    } else {
      (this, (VoteResponse(nodeId, currentTerm, false), List.empty))
    }
  }

  override def onReceive(logState: LogState, config: ClusterConfiguration, msg: VoteResponse): (NodeState, List[Action]) = {
    val votedReceived_ = if (msg.granted) votedReceived + msg.nodeId else votedReceived
    val quorumSize     = (config.members.size + 1) / 2

    if (msg.term > currentTerm)
      (FollowerNode(nodeId, msg.term), List(StoreState))
    else if (msg.term == currentTerm && msg.granted && votedReceived_.size >= quorumSize) {
      val ackedLength = config.members.filterNot(_ == nodeId).map(n => (n, logState.length)).toMap
      val sentLength  = config.members.filterNot(_ == nodeId).map(n => (n, logState.appliedIndex.getOrElse(0L))).toMap
      val actions     = config.members.filterNot(_ == nodeId).map(n => ReplicateLog(n, currentTerm, logState.length)).toList

      (LeaderNode(nodeId, currentTerm, ackedLength, sentLength), StoreState :: AnnounceLeader(nodeId) :: actions)

    } else
      (this.copy(votedReceived = votedReceived_), List.empty)
  }

  override def onReceive(
    logState: LogState,
    config: ClusterConfiguration,
    msg: AppendEntries
  ): (NodeState, (AppendEntriesResponse, List[Action])) = {

    val currentTerm_ = if (msg.term > currentTerm) msg.term else currentTerm

    val logOK_ = msg.logLength >= logState.length
    val logOK = if (logOK_ && msg.logLength > 0) { logState.lastTerm.contains(msg.logTerm) }
    else logOK_

    if (msg.term == currentTerm_ && logOK)
      (
        FollowerNode(nodeId, currentTerm_, None, Some(msg.leaderId)),
        (
          AppendEntriesResponse(nodeId, currentTerm_, msg.logLength + msg.entries.length, true),
          List(StoreState, AnnounceLeader(msg.leaderId))
        )
      )
    else
      (
        this.copy(currentTerm = currentTerm_),
        (
          AppendEntriesResponse(nodeId, currentTerm_, logState.appliedIndex.getOrElse(0), false),
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

  override def leader: Option[String] =
    None

  override def toPersistedState: PersistedState =
    PersistedState(currentTerm, votedFor)

  override def onSnapshotInstalled(logState: LogState, cluster: ClusterConfiguration): (NodeState, AppendEntriesResponse) =
    (this, AppendEntriesResponse(nodeId, currentTerm, logState.appliedIndex.getOrElse(0), false))
}
