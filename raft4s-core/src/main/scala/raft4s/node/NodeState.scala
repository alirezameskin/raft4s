package raft4s.node

import raft4s.Action
import raft4s.log.LogState
import raft4s.protocol.{AppendEntries, AppendEntriesResponse, VoteRequest, VoteResponse}
import raft4s.storage.internal.PersistedState

abstract class NodeState {

  def onReceive(logState: LogState, msg: VoteRequest): (NodeState, (VoteResponse, List[Action]))

  def onReceive(logState: LogState, msg: AppendEntries): (NodeState, (AppendEntriesResponse, List[Action]))

  def onReceive(logState: LogState, msg: VoteResponse): (NodeState, List[Action])

  def onReceive(logState: LogState, msg: AppendEntriesResponse): (NodeState, List[Action])

  def onTimer(logState: LogState): (NodeState, List[Action])

  def onReplicateLog(): List[Action]

  def onSnapshotInstalled(logState: LogState): (NodeState, AppendEntriesResponse)

  def leader: Option[String]

  def toPersistedState: PersistedState
}
