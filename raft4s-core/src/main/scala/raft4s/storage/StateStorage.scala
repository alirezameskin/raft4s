package raft4s.storage

import raft4s.protocol.PersistedState

trait StateStorage[F[_]] {

  def persistState(state: PersistedState): F[Unit]

  def retrieveState(): F[Option[PersistedState]]
}
