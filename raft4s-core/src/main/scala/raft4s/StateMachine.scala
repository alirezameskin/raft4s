package raft4s

import raft4s.protocol.{ReadCommand, WriteCommand}

trait StateMachine[F[_]] {
  def applyWrite: PartialFunction[(Long, WriteCommand[_]), F[Any]]

  def applyRead: PartialFunction[ReadCommand[_], F[Any]]
}
