package raft4s.node

import raft4s.Node
import raft4s.log.LogState
import raft4s.protocol.{PersistedState, _}

case class CandidateNode(
  node: Node,
  currentTerm: Long,
  lastTerm: Long,
  votedFor: Option[Node] = None,
  votedReceived: Set[Node] = Set.empty
) extends NodeState {

  override def onTimer(logState: LogState, config: ClusterConfiguration): (NodeState, List[Action]) = {
    val currentTerm_ = currentTerm + 1
    val lastTerm_    = logState.lastTerm.getOrElse(lastTerm)
    val request      = VoteRequest(node, currentTerm_, logState.length, lastTerm_)
    val actions      = config.members.toList.filterNot(_ == node).map(nodeId => RequestForVote(nodeId, request))
    val quorumSize   = (config.members.size + 1) / 2

    if (1 >= quorumSize) {
      val ackedLength = config.members.filterNot(_ == node).map(n => (n, logState.length)).toMap
      val sentLength  = config.members.filterNot(_ == node).map(n => (n, logState.length)).toMap
      val actions     = config.members.filterNot(_ == node).map(n => ReplicateLog(n, currentTerm, logState.length)).toList

      (LeaderNode(node, currentTerm, ackedLength, sentLength), StoreState :: AnnounceLeader(node) :: actions)
    } else {
      (
        this.copy(currentTerm = currentTerm_, lastTerm = lastTerm_, votedFor = Some(node), votedReceived = Set(node)),
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
        FollowerNode(node, msg.currentTerm, Some(msg.nodeId), None),
        (VoteResponse(node, msg.currentTerm, true), List(StoreState))
      )
    } else {
      (this, (VoteResponse(node, currentTerm, false), List.empty))
    }
  }

  override def onReceive(logState: LogState, config: ClusterConfiguration, msg: VoteResponse): (NodeState, List[Action]) = {
    val votedReceived_ = if (msg.granted) votedReceived + msg.nodeId else votedReceived
    val quorumSize     = (config.members.size + 1) / 2

    if (msg.term > currentTerm)
      (FollowerNode(node, msg.term), List(StoreState))
    else if (msg.term == currentTerm && msg.granted && votedReceived_.size >= quorumSize) {
      val ackedLength = config.members.filterNot(_ == node).map(n => (n, logState.length)).toMap
      val sentLength  = config.members.filterNot(_ == node).map(n => (n, logState.appliedIndex)).toMap
      val actions     = config.members.filterNot(_ == node).map(n => ReplicateLog(n, currentTerm, logState.length)).toList

      (LeaderNode(node, currentTerm, ackedLength, sentLength), StoreState :: AnnounceLeader(node) :: actions)

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
        FollowerNode(node, currentTerm_, None, Some(msg.leaderId)),
        (
          AppendEntriesResponse(node, currentTerm_, msg.logLength + msg.entries.length, true),
          List(StoreState, AnnounceLeader(msg.leaderId))
        )
      )
    else
      (
        this.copy(currentTerm = currentTerm_),
        (
          AppendEntriesResponse(node, currentTerm_, logState.appliedIndex, false),
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
    None

  override def toPersistedState: PersistedState =
    PersistedState(currentTerm, votedFor)

  override def onSnapshotInstalled(logState: LogState, cluster: ClusterConfiguration): (NodeState, AppendEntriesResponse) =
    (this, AppendEntriesResponse(node, currentTerm, logState.appliedIndex, false))
}
