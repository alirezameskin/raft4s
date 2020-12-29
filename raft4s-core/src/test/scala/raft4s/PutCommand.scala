package raft4s

case class PutCommand(key: String, value: String) extends WriteCommand[String]
