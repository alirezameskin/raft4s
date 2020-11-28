package raft4s

import raft4s.rpc.VoteRequest

sealed trait Action

case class RequestForVote(peerId: String, request: VoteRequest)       extends Action
case class ReplicateLog(peerId: String, term: Long, sentLength: Long) extends Action
case class CommitLogs(ackedLength: Map[String, Long], minAckes: Int)  extends Action

case object StartElectionTimer  extends Action
case object CancelElectionTimer extends Action
