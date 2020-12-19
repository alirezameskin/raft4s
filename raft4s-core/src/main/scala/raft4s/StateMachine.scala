package raft4s

import raft4s.protocol.{ReadCommand, WriteCommand}

import java.nio.ByteBuffer

trait StateMachine[F[_]] {
  def applyWrite: PartialFunction[(Long, WriteCommand[_]), F[Any]]

  def applyRead: PartialFunction[ReadCommand[_], F[Any]]

  def appliedIndex: F[Long]

  def takeSnapshot(): F[(Long, ByteBuffer)]

  def restoreSnapshot(index: Long, bytes: ByteBuffer): F[Unit]
}
