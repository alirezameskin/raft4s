package raft4s.node

import raft4s.Node
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
    val lastTerm = logState.lastLogTerm.getOrElse(currentTerm)
    val logOK    = (msg.lastLogTerm > lastTerm) || (msg.lastLogTerm == lastTerm && msg.lastLogIndex >= logState.lastLogIndex)
    val termOK   = msg.term > currentTerm

    if (logOK && termOK)
      (
        FollowerNode(node, msg.term, Some(msg.nodeId)),
        (VoteResponse(node, msg.term, true), List(StoreState, ResetLeaderAnnouncer))
      )
    else {

      val nextIndex_  = nextIndex + (msg.nodeId  -> (msg.lastLogIndex + 1))
      val matchIndex_ = matchIndex + (msg.nodeId -> msg.lastLogIndex)

      (
        this.copy(nextIndex = nextIndex_, matchIndex = matchIndex_),
        (VoteResponse(node, currentTerm, false), List(ReplicateLog(msg.nodeId, currentTerm, msg.lastLogIndex + 1)))
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

      if (msg.prevLogIndex > 0 && localPrvLogEntry.isEmpty)
        (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex, false), actions))
      else if (localPrvLogEntry.isDefined && localPrvLogEntry.get.term != msg.prevLogTerm)
        (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex, false), actions))
      else
        (nextState, (AppendEntriesResponse(node, msg.term, msg.prevLogIndex + msg.entries.length, true), actions))
    } else {

      val nextState = FollowerNode(node, msg.term, currentLeader = Some(msg.leaderId))
      val actions   = List(StoreState, AnnounceLeader(msg.leaderId, true))

      if (msg.prevLogTerm > 0 && localPrvLogEntry.isEmpty)
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
    if (msg.currentTerm > currentTerm) {
      (FollowerNode(node, msg.currentTerm, None, None), List(StoreState, ResetLeaderAnnouncer)) //Convert to Follower
    } else {
      if (msg.success) {
        val nextIndex_  = nextIndex + (msg.nodeId  -> (msg.ack + 1L))
        val matchIndex_ = matchIndex + (msg.nodeId -> msg.ack)

        //If successful: update nextIndex and matchIndex forfollower (§5.3)
        (
          this.copy(matchIndex = matchIndex_, nextIndex = nextIndex_),
          List(CommitLogs(matchIndex_ + (node -> logState.lastLogIndex)))
        )

      } else {
        //If AppendEntries fails because of log inconsistency:decrement nextIndex and retry

        val nodeNextIndex = nextIndex.get(msg.nodeId) match {
          case Some(next) if next == 1 => 1
          case Some(next)              => next - 1
          case None                    => 1
        }

        val nextIndex_ = nextIndex + (msg.nodeId -> nodeNextIndex)
        val actions    = List(ReplicateLog(msg.nodeId, currentTerm, nextIndex_(msg.nodeId)))

        (this.copy(nextIndex = nextIndex_), actions)
      }
    }

  override def onReplicateLog(cluster: ClusterConfiguration): List[Action] =
    cluster.members
      .filterNot(_ == node)
      .map(peer => ReplicateLog(peer, currentTerm, nextIndex.getOrElse(peer, 1L)))
      .toList

  override def leader: Option[Node] =
    Some(node)

  override def toPersistedState: PersistedState =
    PersistedState(currentTerm, Some(node))

  override def onSnapshotInstalled(logState: LogState, cluster: ClusterConfiguration): (NodeState, AppendEntriesResponse) =
    (this, AppendEntriesResponse(node, currentTerm, logState.lastLogIndex - 1, false))

}
