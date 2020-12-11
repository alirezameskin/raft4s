package raft4s

import raft4s.protocol.{ReadCommand, WriteCommand}
import raft4s.storage.Snapshot

trait StateMachine[F[_]] {
  def applyWrite: PartialFunction[(Long, WriteCommand[_]), F[Any]]

  def applyRead: PartialFunction[ReadCommand[_], F[Any]]

  def appliedIndex: F[Long]

  def takeSnapshot(): F[Snapshot]

  def restoreSnapshot(snapshot: Snapshot): F[Unit]
}
