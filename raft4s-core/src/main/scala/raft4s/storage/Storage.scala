package raft4s.storage

import raft4s.log.Log

trait Storage[F[_]] {

  def log: Log[F]

  def persistState(state: PersistedState): F[Unit]

  def retrievePersistedState(): F[Option[PersistedState]]
}
