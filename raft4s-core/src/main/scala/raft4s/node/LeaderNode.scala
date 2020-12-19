package raft4s.node

import raft4s.Node
import raft4s.log.LogState
import raft4s.protocol._

case class LeaderNode(
  node: Node,
  currentTerm: Long,
  matchIndex: Map[Node, Long],
  nextIndex: Map[Node, Long]
) extends NodeState {

  override def onTimer(logState: LogState, config: ClusterConfiguration): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReceive(
    logState: LogState,
    config: ClusterConfiguration,
    msg: VoteRequest
  ): (NodeState, (VoteResponse, List[Action])) = {
    val lastTerm = logState.lastTerm.getOrElse(currentTerm)
    val logOK    = (msg.lastLogTerm > lastTerm) || (msg.lastLogTerm == lastTerm && msg.lastLogIndex >= logState.lastIndex)
    val termOK   = msg.term > currentTerm

    if (logOK && termOK)
      (
        FollowerNode(node, msg.term, Some(msg.nodeId)),
        (VoteResponse(node, msg.term, true), List(StoreState, ResetLeaderAnnouncer))
      )
    else {

      val sentLength_  = nextIndex + (msg.nodeId  -> msg.lastLogIndex)
      val ackedLength_ = matchIndex + (msg.nodeId -> msg.lastLogIndex)

      (
        this.copy(nextIndex = sentLength_, matchIndex = ackedLength_),
        (VoteResponse(node, currentTerm, false), List(ReplicateLog(msg.nodeId, currentTerm, msg.lastLogIndex)))
      )
    }
  }

  override def onReceive(logState: LogState, config: ClusterConfiguration, msg: VoteResponse): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReceive(
    state: LogState,
    config: ClusterConfiguration,
    msg: AppendEntries,
    localPrvLogEntry: Option[LogEntry]
  ): (NodeState, (AppendEntriesResponse, List[Action])) =
    if (msg.term < currentTerm) {
      (this, (AppendEntriesResponse(node, currentTerm, msg.prevLogIndex, false), List.empty))
    } else if (msg.term > currentTerm) {

      val nextState = FollowerNode(node, msg.term, currentLeader = Some(msg.leaderId))
      val actions   = List(StoreState, AnnounceLeader(msg.leaderId, true))

      if (localPrvLogEntry.isEmpty)
        (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex, false), actions))
      else if (localPrvLogEntry.isDefined && localPrvLogEntry.get.term != msg.prevLogTerm)
        (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex, false), actions))
      else
        (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex + msg.entries.length, true), actions))
    } else {

      val nextState = FollowerNode(node, msg.term, currentLeader = Some(msg.leaderId))
      val actions   = List(StoreState, AnnounceLeader(msg.leaderId, true))

      if (localPrvLogEntry.isEmpty)
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
  ): (NodeState, List[Action]) = {
    if (msg.currentTerm > currentTerm) {
      (this, List.empty) //Convert to Follower
    } else {
      if (msg.success) {
        val nextIndex_  = nextIndex + (msg.nodeId  -> msg.ack)
        val matchIndex_ = matchIndex + (msg.nodeId -> msg.ack)

        //If successful: update nextIndex and matchIndex forfollower (§5.3)
      } else {
        val nextIndex_ = nextIndex + (msg.nodeId -> (nextIndex.getOrElse(msg.nodeId, 0L) - 1))

        //If AppendEntries fails because of log inconsistency:decrement nextIndex and retry
      }

    }

    if (msg.currentTerm == currentTerm) {

      if (msg.success && msg.ack >= matchIndex.getOrElse(msg.nodeId, 0L)) {
        val sentLength_  = nextIndex + (msg.nodeId  -> msg.ack)
        val ackedLength_ = matchIndex + (msg.nodeId -> msg.ack)

        (
          this.copy(matchIndex = ackedLength_, nextIndex = sentLength_),
          List(CommitLogs(ackedLength_ + (node -> logState.lastIndex)))
        )

      } else if (nextIndex.getOrElse(msg.nodeId, 0L) > 0) {
        val sentLength_ = nextIndex + (msg.nodeId -> (nextIndex.getOrElse(msg.nodeId, 0L) - 1))
        val actions     = List(ReplicateLog(msg.nodeId, currentTerm, sentLength_(msg.nodeId)))

        (this.copy(nextIndex = sentLength_), actions)
      } else
        (this, List.empty)

    } else if (msg.currentTerm > currentTerm)
      (FollowerNode(node, msg.currentTerm), List(StoreState))
    else
      (this, List(ReplicateLog(msg.nodeId, currentTerm, nextIndex.getOrElse(msg.nodeId, 0L))))
  }

  override def onReplicateLog(cluster: ClusterConfiguration): List[Action] =
    cluster.members
      .filterNot(_ == node)
      .map(peer => ReplicateLog(peer, currentTerm, nextIndex.getOrElse(peer, 0L)))
      .toList

  override def leader: Option[Node] =
    Some(node)

  override def toPersistedState: PersistedState =
    PersistedState(currentTerm, Some(node))

  override def onSnapshotInstalled(logState: LogState, cluster: ClusterConfiguration): (NodeState, AppendEntriesResponse) =
    (this, AppendEntriesResponse(node, currentTerm, logState.lastIndex - 1, false))

}
