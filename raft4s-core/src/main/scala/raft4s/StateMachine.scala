package raft4s

import raft4s.rpc.{ReadCommand, WriteCommand}

trait StateMachine {
  def applyWrite:PartialFunction[(Long, WriteCommand[_]), Any]

  def applyRead: PartialFunction[ReadCommand[_], Any]
}
