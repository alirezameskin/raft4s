package raft4s.storage

case class Storage[F[_]](logStorage: LogStorage[F], stateStorage: StateStorage[F])
