package raft4s.node

import raft4s.{LogEntry, Node}
import raft4s.protocol._
import raft4s.storage.PersistedState

case class FollowerNode(
  node: Node,
  currentTerm: Long,
  votedFor: Option[Node] = None,
  currentLeader: Option[Node] = None
) extends NodeState {

  override def onTimer(logState: LogState, config: ClusterConfiguration): (NodeState, List[Action]) = {
    val (state, actions) = CandidateNode(node, currentTerm, logState.lastLogTerm.getOrElse(0L)).onTimer(logState, config)

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
    if (msg.term < currentTerm) {
      (this, (VoteResponse(node, msg.term, false)))
    } else if (votedFor.isEmpty || votedFor.contains(msg.nodeId)) {
      if (msg.lastLogIndex >= logState.lastLogIndex && msg.lastLogTerm >= logState.lastLogTerm.getOrElse(0L)) {
        (VoteResponse(node, msg.term, true), List(StoreState))
      } else {
        (this, (VoteResponse(node, msg.term, false)))
      }
    }

    val myLogTerm = logState.lastLogTerm.getOrElse(0L)
    val logOK     = (msg.lastLogTerm > myLogTerm) || (msg.lastLogTerm == myLogTerm && msg.lastLogIndex >= logState.lastLogIndex)
    val termOK =
      (msg.term > currentTerm) || (msg.term == currentTerm && (votedFor.isEmpty || votedFor.contains(msg.nodeId)))

    if (logOK && termOK)
      (
        this.copy(currentTerm = msg.term, votedFor = Some(msg.nodeId)),
        (VoteResponse(node, msg.term, true), List(StoreState))
      )
    else
      (this, (VoteResponse(node, currentTerm, false), List.empty))
  }

  override def onReceive(logState: LogState, config: ClusterConfiguration, msg: VoteResponse): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReceive(
    logState: LogState,
    config: ClusterConfiguration,
    msg: AppendEntries,
    localPrevLogEntry: Option[LogEntry]
  ): (NodeState, (AppendEntriesResponse, List[Action])) =
    if (msg.term < currentTerm) {
      (this, (AppendEntriesResponse(node, currentTerm, msg.prevLogIndex, false), List.empty))
    } else {
      if (msg.term > currentTerm) {
        val nextState = this.copy(currentTerm = msg.term, currentLeader = Some(msg.leaderId))
        val actions =
          if (currentLeader.isEmpty)
            List(StoreState, AnnounceLeader(msg.leaderId))
          else if (currentLeader.contains(msg.leaderId))
            List(StoreState)
          else
            List(StoreState, AnnounceLeader(msg.leaderId, true))

        if (msg.prevLogIndex > 0 && localPrevLogEntry.isEmpty)
          (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex, false), actions))
        else if (localPrevLogEntry.isDefined && localPrevLogEntry.get.term != msg.prevLogTerm)
          (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex, false), actions))
        else
          (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex + msg.entries.length, true), actions))

      } else {

        val (nextState, actions) =
          if (currentLeader.isEmpty)
            (this.copy(currentLeader = Some(msg.leaderId)), List(AnnounceLeader(msg.leaderId)))
          else if (currentLeader.contains(msg.leaderId))
            (this, List.empty)
          else
            (this.copy(currentLeader = Some(msg.leaderId)), List(AnnounceLeader(msg.leaderId, true)))

        if (msg.prevLogIndex > 0 && localPrevLogEntry.isEmpty)
          (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex, false), actions))
        else if (localPrevLogEntry.isDefined && localPrevLogEntry.get.term != msg.prevLogTerm) {
          (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex, false), actions))
        } else
          (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex + msg.entries.length, true), actions))
      }
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
    (this, AppendEntriesResponse(node, currentTerm, logState.lastLogIndex - 1, true))
}
