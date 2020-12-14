package raft4s.node

import org.scalatest.flatspec._
import org.scalatest.matchers._
import raft4s.log.LogState
import raft4s.protocol._

class FollowerNodeSpec extends AnyFlatSpec with should.Matchers {
  val nodeId = "node1"
  val nodes  = List("node1", "node2", "node3")

  "A Follower node" should "return an empty list of actions for onReplicateLog request" in {
    val node = FollowerNode(nodeId, nodes, 10)

    node.onReplicateLog() shouldBe List.empty
  }

  it should "return an empty list of actions after getting AppendEntriesResponse without changing the node state" in {
    val node     = FollowerNode(nodeId, nodes, 10)
    val logState = LogState(100, Some(10))

    node.onReceive(logState, AppendEntriesResponse("node2", 10, 1, true)) shouldBe (node, List.empty)
  }

  it should "return an empty list of actions after getting VoteResponse without changing the node state" in {
    val node     = FollowerNode(nodeId, nodes, 10)
    val logState = LogState(100, Some(10))

    node.onReceive(logState, VoteResponse("node2", 10, true)) shouldBe (node, List.empty)
  }

  it should "turn to a Candidate node and with an increased Term and send vote requests on election timer" in {
    val node = FollowerNode(nodeId, nodes, 10)

    val voteRequest     = VoteRequest("node1", 11, 100, 10)
    val voteRequests    = List(RequestForVote("node2", voteRequest), RequestForVote("node3", voteRequest))
    val expectedState   = CandidateNode(nodeId, nodes, 11, 10, Some("node1"), Set("node1"))
    val expectedActions = StoreState :: voteRequests

    val logState = LogState(100, Some(10))

    node.onTimer(logState) shouldBe (expectedState, expectedActions)
  }

  it should "reject any VoteRequest with lower Term and not change its state" in {
    val node     = FollowerNode(nodeId, nodes, 10)
    val logState = LogState(100, Some(10))

    node.onReceive(logState, VoteRequest("node2", 9, 100, 9)) shouldBe (node, (VoteResponse("node1", 10, false), List.empty))
  }

  it should "accept a VoteRequest with higher Term and change its VotedFor and CurrentTerm" in {
    val node     = FollowerNode(nodeId, nodes, 10)
    val logState = LogState(100, Some(10))

    val expectedNode = FollowerNode(nodeId, nodes, 11, Some("node2"), None)

    node.onReceive(logState, VoteRequest("node2", 11, 100, 10)) shouldBe (expectedNode, (
      VoteResponse("node1", 11, true),
      List(StoreState)
    ))
  }

  it should "accept AppendEntries when there is no missing log entry" in {
    val node     = FollowerNode(nodeId, nodes, 10)
    val logState = LogState(100, Some(10))

    val expectedNode = FollowerNode(nodeId, nodes, 10, currentLeader = Some("node2"))
    val request      = AppendEntries("node2", 10, 100, 10, 100, List(LogEntry(10, 101, new WriteCommand[String] {})))

    node.onReceive(logState, request) shouldBe (expectedNode, (
      AppendEntriesResponse("node1", 10, 101, true),
      List(AnnounceLeader("node2"))
    ))
  }

  it should "reject AppendEntries when there is at least one missed log entry" in {
    val node     = FollowerNode(nodeId, nodes, 10)
    val logState = LogState(100, Some(10))

    val expectedNode = FollowerNode(nodeId, nodes, 10)
    val request      = AppendEntries("node2", 10, 105, 10, 105, List(LogEntry(10, 106, new WriteCommand[String] {})))

    node.onReceive(logState, request) shouldBe (expectedNode, (AppendEntriesResponse("node1", 10, 0, false), List.empty))

  }
}
