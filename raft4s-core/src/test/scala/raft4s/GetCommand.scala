package raft4s

case class GetCommand(key: String) extends ReadCommand[String]
