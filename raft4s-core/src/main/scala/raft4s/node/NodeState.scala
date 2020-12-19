package raft4s.node

import raft4s.Node
import raft4s.log.LogState
import raft4s.protocol._

abstract class NodeState {

  def onTimer(logState: LogState, config: ClusterConfiguration): (NodeState, List[Action])

  def onReceive(logState: LogState, config: ClusterConfiguration, msg: VoteRequest): (NodeState, (VoteResponse, List[Action]))

  def onReceive(
    state: LogState,
    config: ClusterConfiguration,
    msg: AppendEntries,
    localPrvLogEntry: Option[LogEntry]
  ): (NodeState, (AppendEntriesResponse, List[Action]))

  def onReceive(logState: LogState, config: ClusterConfiguration, msg: VoteResponse): (NodeState, List[Action])

  def onReceive(logState: LogState, config: ClusterConfiguration, msg: AppendEntriesResponse): (NodeState, List[Action])

  def onReplicateLog(config: ClusterConfiguration): List[Action]

  def onSnapshotInstalled(logState: LogState, config: ClusterConfiguration): (NodeState, AppendEntriesResponse)

  def leader: Option[Node]

  def toPersistedState: PersistedState
}
