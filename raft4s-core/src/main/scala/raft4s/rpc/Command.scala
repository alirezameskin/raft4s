package raft4s.rpc

sealed trait Command[OUT]
trait ReadCommand[OUT]  extends Command[OUT]
trait WriteCommand[OUT] extends Command[OUT]
