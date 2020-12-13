package raft4s.storage

trait SnapshotStorage[F[_]] {

  def saveSnapshot(snapshot: Snapshot): F[Unit]

  def retrieveSnapshot(): F[Option[Snapshot]]

  def getLatestSnapshot(): F[Option[Snapshot]]
}
