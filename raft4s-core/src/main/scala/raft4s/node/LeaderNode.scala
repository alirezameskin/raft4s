package raft4s.node

import raft4s.Node
import raft4s.log.LogState
import raft4s.protocol._

case class LeaderNode(
  node: Node,
  currentTerm: Long,
  ackedLength: Map[Node, Long],
  sentLength: Map[Node, Long]
) extends NodeState {

  override def onTimer(logState: LogState, config: ClusterConfiguration): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReceive(
    logState: LogState,
    config: ClusterConfiguration,
    msg: VoteRequest
  ): (NodeState, (VoteResponse, List[Action])) = {
    val lastTerm = logState.lastTerm.getOrElse(currentTerm)
    val logOK    = (msg.logTerm > lastTerm) || (msg.logTerm == lastTerm && msg.logLength >= logState.length)
    val termOK   = msg.currentTerm > currentTerm

    if (logOK && termOK)
      (
        FollowerNode(node, msg.currentTerm, Some(msg.nodeId)),
        (VoteResponse(node, msg.currentTerm, true), List(StoreState, ResetLeaderAnnouncer))
      )
    else {

      val sentLength_  = sentLength + (msg.nodeId  -> msg.logLength)
      val ackedLength_ = ackedLength + (msg.nodeId -> msg.logLength)

      (
        this.copy(sentLength = sentLength_, ackedLength = ackedLength_),
        (VoteResponse(node, currentTerm, false), List(ReplicateLog(msg.nodeId, currentTerm, msg.logLength)))
      )
    }
  }

  override def onReceive(logState: LogState, config: ClusterConfiguration, msg: VoteResponse): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReceive(
    logState: LogState,
    config: ClusterConfiguration,
    msg: AppendEntries
  ): (NodeState, (AppendEntriesResponse, List[Action])) = {
    val currentTerm_ = if (msg.term > currentTerm) msg.term else currentTerm
    val logOK_       = logState.length >= msg.logLength
    val logOK        = if (logOK_ && msg.logLength > 0) logState.lastTerm.contains(msg.logTerm) else logOK_

    if (msg.term == currentTerm_ && logOK)
      (
        FollowerNode(node, currentTerm_, None, Some(msg.leaderId)),
        (
          AppendEntriesResponse(node, currentTerm_, msg.logLength + msg.entries.length, true),
          List(StoreState, AnnounceLeader(msg.leaderId, true))
        )
      )
    else if (msg.term > currentTerm)
      (
        FollowerNode(node, currentTerm_, None, Some(msg.leaderId)),
        (
          AppendEntriesResponse(node, currentTerm_, logState.length, false),
          List(StoreState, AnnounceLeader(msg.leaderId, true))
        )
      )
    else
      (this, (AppendEntriesResponse(node, currentTerm, logState.length, false), List.empty))
  }

  override def onReceive(
    logState: LogState,
    cluster: ClusterConfiguration,
    msg: AppendEntriesResponse
  ): (NodeState, List[Action]) =
    if (msg.currentTerm == currentTerm) {

      if (msg.success && msg.ack >= ackedLength.getOrElse(msg.nodeId, 0L)) {
        val sentLength_  = sentLength + (msg.nodeId  -> msg.ack)
        val ackedLength_ = ackedLength + (msg.nodeId -> msg.ack)

        (
          this.copy(ackedLength = ackedLength_, sentLength = sentLength_),
          List(CommitLogs(ackedLength_ + (node -> logState.length)))
        )

      } else if (sentLength.getOrElse(msg.nodeId, 0L) > 0) {
        val sentLength_ = sentLength + (msg.nodeId -> (sentLength.getOrElse(msg.nodeId, 0L) - 1))
        val actions     = List(ReplicateLog(msg.nodeId, currentTerm, sentLength_(msg.nodeId)))

        (this.copy(sentLength = sentLength_), actions)
      } else
        (this, List.empty)

    } else if (msg.currentTerm > currentTerm)
      (FollowerNode(node, msg.currentTerm), List(StoreState))
    else
      (this, List(ReplicateLog(msg.nodeId, currentTerm, sentLength.getOrElse(msg.nodeId, 0L))))

  override def onReplicateLog(cluster: ClusterConfiguration): List[Action] =
    cluster.members
      .filterNot(_ == node)
      .map(peer => ReplicateLog(peer, currentTerm, sentLength.getOrElse(peer, 0L)))
      .toList

  override def leader: Option[Node] =
    Some(node)

  override def toPersistedState: PersistedState =
    PersistedState(currentTerm, Some(node))

  override def onSnapshotInstalled(logState: LogState, cluster: ClusterConfiguration): (NodeState, AppendEntriesResponse) =
    (this, AppendEntriesResponse(node, currentTerm, logState.length - 1, false))
}
