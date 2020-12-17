package raft4s.protocol

sealed private[raft4s] trait Action

case class RequestForVote(peerId: Node, request: VoteRequest)             extends Action
case class ReplicateLog(peerId: Node, term: Long, sentLength: Long)       extends Action
case class CommitLogs(ackedLength: Map[Node, Long])                       extends Action
case class AnnounceLeader(leaderId: Node, resetPrevious: Boolean = false) extends Action
case object ResetLeaderAnnouncer                                            extends Action
case object StoreState                                                      extends Action
