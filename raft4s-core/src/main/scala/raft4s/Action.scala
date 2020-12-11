package raft4s

import raft4s.protocol.VoteRequest
import raft4s.storage.Snapshot

sealed trait Action

case class RequestForVote(peerId: String, request: VoteRequest)             extends Action
case class ReplicateLog(peerId: String, term: Long, sentLength: Long)       extends Action
case class CommitLogs(ackedLength: Map[String, Long], minAckes: Int)        extends Action
case class AnnounceLeader(leaderId: String, resetPrevious: Boolean = false) extends Action
case object ResetLeaderAnnouncer                                            extends Action
case object StoreState                                                      extends Action
case class SendSnapshot(peerId: String, snapshot: Snapshot)                 extends Action
