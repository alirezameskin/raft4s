package raft4s

import raft4s.storage.{LogStorage, SnapshotStorage, StateStorage}

case class Storage[F[_]](logStorage: LogStorage[F], stateStorage: StateStorage[F], snapshotStorage: SnapshotStorage[F])
