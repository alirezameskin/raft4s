package raft4s.protocol

sealed private[raft4s] trait Action

case class RequestForVote(peerId: String, request: VoteRequest)             extends Action
case class ReplicateLog(peerId: String, term: Long, sentLength: Long)       extends Action
case class CommitLogs(ackedLength: Map[String, Long], minAckes: Int)        extends Action
case class AnnounceLeader(leaderId: String, resetPrevious: Boolean = false) extends Action
case object ResetLeaderAnnouncer                                            extends Action
case object StoreState                                                      extends Action
