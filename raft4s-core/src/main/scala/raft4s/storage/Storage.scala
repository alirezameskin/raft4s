package raft4s.storage

import raft4s.log.Log
import raft4s.node.NodeState

trait Storage[F[_]] {

  def log: Log[F]

  def persistState(state: NodeState): F[Unit]

  def retrievePersistedState(): F[PersistedState]
}
