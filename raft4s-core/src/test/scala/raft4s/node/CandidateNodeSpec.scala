package raft4s.node

import org.scalatest.flatspec._
import org.scalatest.matchers._
import raft4s.{ReplicateLog, RequestForVote}
import raft4s.log.LogState
import raft4s.protocol.{AppendEntries, AppendEntriesResponse, LogEntry, VoteRequest, VoteResponse, WriteCommand}

class CandidateNodeSpec extends AnyFlatSpec with should.Matchers {
  val nodeId = "node1"
  val nodes  = List("node1", "node2", "node3")

  "A Candidate node" should "return an empty list of actions for onReplicateLog request" in {
    val node = CandidateNode(nodeId, nodes, 10, 10, Some("node1"), Set("node1"))

    node.onReplicateLog() shouldBe List.empty
  }

  it should "return an empty list of actions after getting AppendEntriesResponse without changing the node state" in {
    val node     = CandidateNode(nodeId, nodes, 10, 10, Some("node1"), Set("node1"))
    val logState = LogState(100, Some(10))

    node.onReceive(logState, AppendEntriesResponse("node2", 10, 1, true)) shouldBe (node, List.empty)
  }

  it should "stay in Candidate state and increase the term and send vote requests on election timer" in {
    val node = CandidateNode(nodeId, nodes, 10, 10, Some("node1"), Set("node1"))

    val voteRequest     = VoteRequest("node1", 11, 100, 10)
    val voteRequests    = List(RequestForVote("node2", voteRequest), RequestForVote("node3", voteRequest))
    val expectedState   = CandidateNode(nodeId, nodes, 11, 10, Some("node1"), Set("node1"))
    val expectedActions = voteRequests

    val logState = LogState(100, Some(10))

    node.onTimer(logState) shouldBe (expectedState, expectedActions)
  }

  it should "turn to a FollowerNode after discovering a higher Term, and accept the received VoteRequest" in {

    val node     = CandidateNode(nodeId, nodes, 10, 10, Some("node1"), Set("node1"))
    val logState = LogState(100, Some(10))

    val expectedState = FollowerNode("node1", nodes, 11, Some("node2"), None)

    node.onReceive(logState, VoteRequest("node2", 11, 100, 10)) shouldBe (expectedState, VoteResponse("node1", 11, true))
  }

  it should "reject any VoteRequest with lower Term and not change its state" in {
    val node     = CandidateNode(nodeId, nodes, 10, 10, Some("node1"), Set("node1"))
    val logState = LogState(100, Some(10))

    node.onReceive(logState, VoteRequest("node2", 9, 100, 9)) shouldBe (node, VoteResponse("node1", 10, false))
  }

  it should "reject any VoteRequest with lower log length and not change its state" in {
    val node     = CandidateNode(nodeId, nodes, 10, 10, Some("node1"), Set("node1"))
    val logState = LogState(100, Some(10))

    node.onReceive(logState, VoteRequest("node2", 10, 99, 10)) shouldBe (node, VoteResponse("node1", 10, false))
  }

  it should "turn to a Follower node and cancel Election timer when receiving a VoteResponse with higher Term" in {
    val node     = CandidateNode(nodeId, nodes, 10, 10, Some("node1"), Set("node1"))
    val logState = LogState(100, Some(10))

    val expectedState = FollowerNode(nodeId, nodes, 11, None, None)
    node.onReceive(logState, VoteResponse("node2", 11, false)) shouldBe (expectedState, List.empty)
  }

  it should "stay in the candidate state and adder the voter id in the Voted list" in {
    val node     = CandidateNode(nodeId, List("node1", "node2", "node3", "node4", "node5"), 10, 10, Some("node1"), Set("node1"))
    val logState = LogState(100, Some(10))

    val expectedState = node.copy(votedReceived = Set("node1", "node2"))

    node.onReceive(logState, VoteResponse("node2", 10, true)) shouldBe (expectedState, List.empty)
  }

  it should "not consider duplicate vote response" in {
    val node     = CandidateNode(nodeId, List("node1", "node2", "node3", "node4", "node5"), 10, 10, Some("node1"), Set("node1"))
    val logState = LogState(100, Some(10))

    val expectedState = node.copy(votedReceived = Set("node1", "node2"))

    node.onReceive(logState, VoteResponse("node2", 10, true)) shouldBe (expectedState, List.empty)
    expectedState.onReceive(logState, VoteResponse("node2", 10, true)) shouldBe (expectedState, List.empty)
  }

  it should "turn to a Leader and start log replication after receiving a quorum of granted VoteResponse" in {
    val node     = CandidateNode(nodeId, nodes, 10, 10, Some("node1"), Set("node1"))
    val logState = LogState(100, Some(10))

    val expectedState = LeaderNode(
      nodeId,
      nodes,
      10,
      sentLength = Map("node2" -> 0, "node3" -> 0),
      ackedLength = Map("node2" -> 100, "node3" -> 100)
    )

    val expectedActions = List(ReplicateLog("node2", 10, 0), ReplicateLog("node3", 10, 0))

    node.onReceive(logState, VoteResponse("node2", 10, true)) shouldBe (expectedState, expectedActions)
  }

  it should "turn to a Follower node after receiving an AppendEntries request with higher Term" in {
    val node     = CandidateNode(nodeId, nodes, 10, 10, Some("node1"), Set("node1"))
    val logState = LogState(100, Some(10))

    val command = new WriteCommand[String] {}
    val request =
      AppendEntries("node2", term = 11, logLength = 100, logTerm = 10, leaderCommit = 100, List(LogEntry(11, 101, command)))

    val expectedResponse = AppendEntriesResponse(nodeId, 11, 101, true)
    val expectedState    = FollowerNode(nodeId, nodes, 11, None, Some("node2"))

    node.onReceive(logState, request) shouldBe (expectedState, expectedResponse)
  }

  it should "stays in Candidate state and reject the AppendEntries request after receiving AppendEntries with lower Term" in {
    val node     = CandidateNode(nodeId, nodes, 10, 10, Some("node1"), Set("node1"))
    val logState = LogState(100, Some(10))

    val command = new WriteCommand[String] {}
    val request =
      AppendEntries("node2", term = 10, logLength = 99, logTerm = 10, leaderCommit = 99, List(LogEntry(10, 100, command)))

    node.onReceive(logState, request) shouldBe (node, AppendEntriesResponse(nodeId, 10, 0, false))
  }
}
