package raft4s

import raft4s.protocol.VoteRequest

sealed trait Action

case class RequestForVote(peerId: String, request: VoteRequest)       extends Action
case class ReplicateLog(peerId: String, term: Long, sentLength: Long) extends Action
case class CommitLogs(ackedLength: Map[String, Long], minAckes: Int)  extends Action
