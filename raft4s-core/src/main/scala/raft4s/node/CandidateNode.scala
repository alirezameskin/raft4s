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
    val lastTerm_    = logState.lastLogTerm.getOrElse(lastTerm)
    val request      = VoteRequest(node, currentTerm_, logState.lastLogIndex, lastTerm_)
    val actions      = config.members.toList.filterNot(_ == node).map(nodeId => RequestForVote(nodeId, request))
    val quorumSize   = (config.members.size + 1) / 2

    if (1 >= quorumSize) {
      val matchIndex = config.members.filterNot(_ == node).map(n => (n, 0L)).toMap
      val nextIndex  = config.members.filterNot(_ == node).map(n => (n, logState.lastLogIndex + 1)).toMap
      val actions    = config.members.filterNot(_ == node).map(n => ReplicateLog(n, currentTerm, logState.lastLogIndex + 1)).toList

      (LeaderNode(node, currentTerm, matchIndex, nextIndex), StoreState :: AnnounceLeader(node) :: actions)
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

    val myLogTerm = logState.lastLogTerm.getOrElse(0L)
    val logOK     = (msg.lastLogTerm > myLogTerm) || (msg.lastLogTerm == myLogTerm && msg.lastLogIndex >= logState.lastLogIndex)
    val termOK =
      (msg.term > currentTerm) || (msg.term == currentTerm && (votedFor.isEmpty || votedFor.contains(msg.nodeId)))

    if (logOK && termOK) {
      (
        FollowerNode(node, msg.term, Some(msg.nodeId), None),
        (VoteResponse(node, msg.term, true), List(StoreState))
      )
    } else {
      (this, (VoteResponse(node, currentTerm, false), List.empty))
    }
  }

  override def onReceive(logState: LogState, config: ClusterConfiguration, msg: VoteResponse): (NodeState, List[Action]) = {
    val votedReceived_ = if (msg.voteGranted) votedReceived + msg.nodeId else votedReceived
    val quorumSize     = (config.members.size + 1) / 2

    if (msg.term > currentTerm)
      (FollowerNode(node, msg.term), List(StoreState))
    else if (msg.term == currentTerm && msg.voteGranted && votedReceived_.size >= quorumSize) {
      val matchIndex = config.members.filterNot(_ == node).map(n => (n, 0L)).toMap
      val nextIndex  = config.members.filterNot(_ == node).map(n => (n, logState.lastLogIndex + 1)).toMap
      val actions    = config.members.filterNot(_ == node).map(n => ReplicateLog(n, currentTerm, logState.lastLogIndex + 1)).toList

      (LeaderNode(node, currentTerm, matchIndex, nextIndex), StoreState :: AnnounceLeader(node) :: actions)

    } else
      (this.copy(votedReceived = votedReceived_), List.empty)
  }

  override def onReceive(
    logState: LogState,
    config: ClusterConfiguration,
    msg: AppendEntries,
    localPrvLogEntry: Option[LogEntry]
  ): (NodeState, (AppendEntriesResponse, List[Action])) =
    if (msg.term < currentTerm) {
      (this, (AppendEntriesResponse(node, currentTerm, msg.prevLogIndex, false), List.empty))
    } else if (msg.term > currentTerm) {

      val nextState = FollowerNode(node, msg.term, currentLeader = Some(msg.leaderId))
      val actions   = List(StoreState, AnnounceLeader(msg.leaderId))

      if (msg.prevLogIndex > 0 && localPrvLogEntry.isEmpty)
        (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex, false), actions))
      else if (localPrvLogEntry.isDefined && localPrvLogEntry.get.term != msg.prevLogTerm)
        (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex, false), actions))
      else
        (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex + msg.entries.length, true), actions))
    } else {

      val nextState = FollowerNode(node, msg.term, currentLeader = Some(msg.leaderId))
      val actions   = List(StoreState, AnnounceLeader(msg.leaderId))

      if (msg.prevLogIndex > 0 && localPrvLogEntry.isEmpty)
        (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex, false), actions))
      else if (localPrvLogEntry.isDefined && localPrvLogEntry.get.term != msg.prevLogTerm) {
        (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex, false), actions))
      } else
        (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex + msg.entries.length, true), actions))
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
    (this, AppendEntriesResponse(node, currentTerm, logState.lastAppliedIndex, false))
}
