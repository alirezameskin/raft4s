package raft4s.node

import org.scalatest.flatspec._
import org.scalatest.matchers._
import raft4s.log.LogState
import raft4s.protocol._
import raft4s.Node

class FollowerNodeSpec extends AnyFlatSpec with should.Matchers {
  val node1  = Node("node1", 1080)
  val node2  = Node("node2", 1080)
  val node3  = Node("node3", 1080)
  val nodes  = Set(node1, node2, node3)
  val config = NewClusterConfiguration(nodes)

  "A Follower node" should "return an empty list of actions for onReplicateLog request" in {
    val node = FollowerNode(node1, 10)

    node.onReplicateLog(config) shouldBe List.empty
  }

  it should "return an empty list of actions after getting AppendEntriesResponse without changing the node state" in {
    val node     = FollowerNode(node1, 10)
    val logState = LogState(100, Some(10))

    node.onReceive(logState, config, AppendEntriesResponse(node2, 10, 1, true)) shouldBe (node, List.empty)
  }

  it should "return an empty list of actions after getting VoteResponse without changing the node state" in {
    val node     = FollowerNode(node1, 10)
    val logState = LogState(100, Some(10))

    node.onReceive(logState, config, VoteResponse(node2, 10, true)) shouldBe (node, List.empty)
  }

  it should "turn to a Candidate node and with an increased Term and send vote requests on election timer" in {
    val node = FollowerNode(node1, 10)

    val voteRequest     = VoteRequest(node1, 11, 100, 10)
    val voteRequests    = List(RequestForVote(node2, voteRequest), RequestForVote(node3, voteRequest))
    val expectedState   = CandidateNode(node1, 11, 10, Some(node1), Set(node1))
    val expectedActions = StoreState :: voteRequests

    val logState = LogState(100, Some(10))

    node.onTimer(logState, config) shouldBe (expectedState, expectedActions)
  }

  it should "reject any VoteRequest with lower Term and not change its state" in {
    val node     = FollowerNode(node1, 10)
    val logState = LogState(100, Some(10))

    node.onReceive(logState, config, VoteRequest(node2, 9, 100, 9)) shouldBe (node, (VoteResponse(node1, 10, false), List.empty))
  }

  it should "accept a VoteRequest with higher Term and change its VotedFor and CurrentTerm" in {
    val node     = FollowerNode(node1, 10)
    val logState = LogState(100, Some(10))

    val expectedNode = FollowerNode(node1, 11, Some(node2), None)

    node.onReceive(logState, config, VoteRequest(node2, 11, 100, 10)) shouldBe (expectedNode, (
      VoteResponse(node1, 11, true),
      List(StoreState)
    ))
  }

  it should "accept AppendEntries when there is no missing log entry" in {
    val node     = FollowerNode(node1, 10)
    val logState = LogState(100, Some(10))
    val prevLog  = Some(LogEntry(10, 100, new WriteCommand[String] {}))

    val expectedNode = FollowerNode(node1, 10, currentLeader = Some(node2))
    val request      = AppendEntries(node2, 10, 100, 10, 100, List(LogEntry(10, 101, new WriteCommand[String] {})))

    node.onReceive(logState, config, request, prevLog) shouldBe (expectedNode, (
      AppendEntriesResponse(node1, 10, 101, true),
      List(AnnounceLeader(node2))
    ))
  }

  it should "reject AppendEntries when there is at least one missed log entry" in {
    val node     = FollowerNode(node1, 10, currentLeader = Some(node2))
    val logState = LogState(100, Some(10))
    val prevLog  = None

    val expectedNode = FollowerNode(node1, 10, currentLeader = Some(node2))
    val request      = AppendEntries(node2, 10, 105, 10, 105, List(LogEntry(10, 106, new WriteCommand[String] {})))

    node.onReceive(logState, config, request, prevLog) shouldBe (expectedNode, (
      AppendEntriesResponse(node1, 10, 105, false),
      List.empty
    ))

  }

  it should "Reject" in {
    val node     = FollowerNode(node2, 15, currentLeader = Some(node1))
    val logState = LogState(35, Some(12), 33)
    val nullCmd  = new WriteCommand[String] {}
    val prevLog  = Some(LogEntry(12, 33, new WriteCommand[String] {}))

    val request = AppendEntries(
      node1,
      15,
      33,
      12,
      34,
      List(
        LogEntry(12, 34, nullCmd),
        LogEntry(12, 35, nullCmd),
        LogEntry(13, 36, nullCmd),
        LogEntry(15, 37, nullCmd)
      )
    )

    val expectedNode = node.copy(currentLeader = Some(node1))
    node.onReceive(logState, config, request, prevLog) shouldBe (expectedNode, (
      AppendEntriesResponse(node2, 15, 37, true),
      List.empty
    ))
  }

  it should "reject - 2" in {
    val node     = FollowerNode(node2, 25, Some(node1), Some(node1))
    val logState = LogState(45, Some(25), 43)
    val nullCmd  = new WriteCommand[String] {}
    val prevLog  = Some(LogEntry(19, 40, new WriteCommand[String] {}))

    val request = AppendEntries(
      node1,
      25,
      40,
      19,
      43,
      List(
        LogEntry(23, 41, nullCmd),
        LogEntry(23, 42, nullCmd),
        LogEntry(25, 43, nullCmd),
        LogEntry(25, 44, nullCmd)
      )
    )

    node.onReceive(logState, config, request, prevLog) shouldBe (node, (AppendEntriesResponse(node2, 25, 44, true), List.empty))
  }

  it should "reject - 3" in {
    val node     = FollowerNode(node2, 27, Some(node1), Some(node1))
    val logState = LogState(46, Some(27), 43)
    val request  = AppendEntries(node1, 27, 45, 25, 43, List())
    val prevLog  = Some(LogEntry(10, 100, new WriteCommand[String] {}))

    node.onReceive(logState, config, request, prevLog) shouldBe (node, (AppendEntriesResponse(node2, 27, 45, false), List.empty))
  }
}
