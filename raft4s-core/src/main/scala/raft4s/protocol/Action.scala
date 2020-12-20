package raft4s.protocol

import raft4s.Node

sealed private[raft4s] trait Action

private[raft4s] case class RequestForVote(peerId: Node, request: VoteRequest)             extends Action
private[raft4s] case class ReplicateLog(peerId: Node, term: Long, nextIndex: Long)        extends Action
private[raft4s] case class CommitLogs(matchIndex: Map[Node, Long])                        extends Action
private[raft4s] case class AnnounceLeader(leaderId: Node, resetPrevious: Boolean = false) extends Action
private[raft4s] case object ResetLeaderAnnouncer                                          extends Action
private[raft4s] case object StoreState                                                    extends Action
