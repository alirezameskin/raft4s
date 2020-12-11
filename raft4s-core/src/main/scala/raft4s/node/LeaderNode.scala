package raft4s.node

import raft4s.log.LogState
import raft4s.protocol.{AppendEntries, AppendEntriesResponse, VoteRequest, VoteResponse}
import raft4s.storage.internal.PersistedState
import raft4s.{Action, AnnounceLeader, CommitLogs, ReplicateLog, SendSnapshot, StoreState}

case class LeaderNode(
  nodeId: String,
  nodes: List[String],
  currentTerm: Long,
  ackedLength: Map[String, Long],
  sentLength: Map[String, Long]
) extends NodeState {

  override def onTimer(logState: LogState): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReceive(logState: LogState, msg: VoteRequest): (NodeState, (VoteResponse, List[Action])) = {
    val lastTerm = logState.lastTerm.getOrElse(currentTerm)
    val logOK    = (msg.logTerm > lastTerm) || (msg.logTerm == lastTerm && msg.logLength >= logState.length)
    val termOK   = msg.currentTerm >= currentTerm

    if (logOK && termOK)
      (
        FollowerNode(nodeId, nodes, msg.currentTerm, Some(msg.nodeId)),
        (VoteResponse(nodeId, msg.currentTerm, true), List(StoreState))
      )
    else
      (this, (VoteResponse(nodeId, msg.currentTerm, false), List.empty))
  }

  override def onReceive(logState: LogState, msg: VoteResponse): (NodeState, List[Action]) =
    (this, List.empty)

  override def onReceive(logState: LogState, msg: AppendEntries): (NodeState, (AppendEntriesResponse, List[Action])) = {
    val currentTerm_ = if (msg.term > currentTerm) msg.term else currentTerm
    val logOK_       = logState.length >= msg.logLength
    val logOK        = if (logOK_ && msg.logLength > 0) logState.lastTerm.contains(msg.logTerm) else logOK_

    if (msg.term == currentTerm_ && logOK)
      (
        FollowerNode(nodeId, nodes, currentTerm_, None, Some(msg.leaderId)),
        (
          AppendEntriesResponse(nodeId, currentTerm_, msg.logLength + msg.entries.length, true),
          List(StoreState, AnnounceLeader(msg.leaderId))
        )
      )
    else
      (this, (AppendEntriesResponse(nodeId, currentTerm, logState.length, false), List.empty))
  }

  override def onReceive(logState: LogState, msg: AppendEntriesResponse): (NodeState, List[Action]) =
    if (msg.currentTerm == currentTerm)
      if (msg.success && msg.ack >= ackedLength(msg.nodeId)) {
        val sentLength_  = sentLength + (msg.nodeId  -> msg.ack)
        val ackedLength_ = ackedLength + (msg.nodeId -> msg.ack)

        (
          this.copy(ackedLength = ackedLength_, sentLength = sentLength_),
          List(CommitLogs(ackedLength_ + (nodeId -> logState.length), (nodes.size + 1) / 2))
        )

      } else if (logState.snapshot.isDefined && msg.ack < logState.snapshot.map(_.lastIndex).getOrElse(0L)) {
        val sentLength_ = sentLength + (msg.nodeId -> logState.snapshot.map(_.lastIndex).getOrElse(0L))
        val actions     = List(SendSnapshot(msg.nodeId, logState.snapshot.get))

        (this.copy(sentLength = sentLength_), actions)

      } else if (sentLength(msg.nodeId) > 0) {
        val sentLength_ = sentLength + (msg.nodeId -> (sentLength(msg.nodeId) - 1))
        val actions     = List(ReplicateLog(msg.nodeId, currentTerm, sentLength_(msg.nodeId)))

        (this.copy(sentLength = sentLength_), actions)
      } else
        (this, List.empty)
    else if (msg.currentTerm > currentTerm)
      (FollowerNode(nodeId, nodes, msg.currentTerm), List(StoreState))
    else
      (this, List.empty)

  override def onReplicateLog(): List[Action] =
    nodes.filterNot(_ == nodeId).map { peer =>
      ReplicateLog(peer, currentTerm, sentLength(peer))
    }

  override def leader: Option[String] =
    Some(nodeId)

  override def toPersistedState: PersistedState =
    PersistedState(currentTerm, Some(nodeId))

  override def onSnapshotInstalled(logState: LogState): (NodeState, AppendEntriesResponse) =
    (this, AppendEntriesResponse(nodeId, currentTerm, logState.length - 1, false))
}
