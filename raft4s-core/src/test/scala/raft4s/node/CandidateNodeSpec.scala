package raft4s.node

import org.scalatest.flatspec._
import org.scalatest.matchers._
import raft4s.Node
import raft4s.log.LogState
import raft4s.protocol._

class CandidateNodeSpec extends AnyFlatSpec with should.Matchers {
  val node1  = Node("node1", 1080)
  val node2  = Node("node2", 1080)
  val node3  = Node("node3", 1080)
  val nodes  = Set(node1, node2, node3)
  val config = NewClusterConfiguration(nodes)

  "A Candidate node" should "return an empty list of actions for onReplicateLog request" in {
    val node = CandidateNode(node1, 10, 10, Some(node1), Set(node1))

    node.onReplicateLog(config) shouldBe List.empty
  }

  it should "return an empty list of actions after getting AppendEntriesResponse without changing the node state" in {
    val node     = CandidateNode(node1, 10, 10, Some(node1), Set(node1))
    val logState = LogState(100, Some(10))

    node.onReceive(logState, config, AppendEntriesResponse(node2, 10, 1, true)) shouldBe (node, List.empty)
  }

  it should "stay in Candidate state and increase the term and send vote requests on election timer" in {
    val node = CandidateNode(node1, 10, 10, Some(node1), Set(node1))

    val voteRequest     = VoteRequest(node1, 11, 100, 10)
    val voteRequests    = List(RequestForVote(node2, voteRequest), RequestForVote(node3, voteRequest))
    val expectedState   = CandidateNode(node1, 11, 10, Some(node1), Set(node1))
    val expectedActions = StoreState :: voteRequests

    val logState = LogState(100, Some(10))

    node.onTimer(logState, config) shouldBe (expectedState, expectedActions)
  }

  it should "turn to a FollowerNode after discovering a higher Term, and accept the received VoteRequest" in {

    val node     = CandidateNode(node1, 10, 10, Some(node1), Set(node1))
    val logState = LogState(100, Some(10))

    val expectedState = FollowerNode(node1, 11, Some(node2), None)

    node.onReceive(logState, config, VoteRequest(node2, 11, 100, 10)) shouldBe (expectedState, (
      VoteResponse(node1, 11, true),
      List(StoreState)
    ))
  }

  it should "reject any VoteRequest with lower Term and not change its state" in {
    val node     = CandidateNode(node1, 10, 10, Some(node1), Set(node1))
    val logState = LogState(100, Some(10))

    node.onReceive(logState, config, VoteRequest(node2, 9, 100, 9)) shouldBe (node, (VoteResponse(node1, 10, false), List.empty))
  }

  it should "reject any VoteRequest with lower log length and not change its state" in {
    val node     = CandidateNode(node1, 10, 10, Some(node1), Set(node1))
    val logState = LogState(100, Some(10))

    node.onReceive(logState, config, VoteRequest(node2, 10, 99, 10)) shouldBe (node, (VoteResponse(node1, 10, false), List.empty))
  }

  it should "turn to a Follower node and cancel Election timer when receiving a VoteResponse with higher Term" in {
    val node     = CandidateNode(node1, 10, 10, Some(node1), Set(node1))
    val logState = LogState(100, Some(10))

    val expectedState = FollowerNode(node1, 11, None, None)
    node.onReceive(logState, config, VoteResponse(node2, 11, false)) shouldBe (expectedState, List(StoreState))
  }

  it should "stay in the candidate state and adder the voter id in the Voted list" in {
    val node     = CandidateNode(node1, 10, 10, Some(node1), Set(node1))
    val logState = LogState(100, Some(10))
    val config   = NewClusterConfiguration(Set(node1, node2, node3, Node("node4", 1080), Node("node5", 1080)))

    val expectedState = node.copy(votedReceived = Set(node1, node2))

    node.onReceive(logState, config, VoteResponse(node2, 10, true)) shouldBe (expectedState, List.empty)
  }

  it should "not consider duplicate vote response" in {
    val node     = CandidateNode(node1, 10, 10, Some(node1), Set(node1))
    val logState = LogState(100, Some(10))
    val config   = NewClusterConfiguration(Set(node1, node2, node3, Node("node4", 1080), Node("node5", 1080)))

    val expectedState = node.copy(votedReceived = Set(node1, node2))

    node.onReceive(logState, config, VoteResponse(node2, 10, true)) shouldBe (expectedState, List.empty)
    expectedState.onReceive(logState, config, VoteResponse(node2, 10, true)) shouldBe (expectedState, List.empty)
  }

  it should "turn to a Leader and start log replication after receiving a quorum of granted VoteResponse" in {
    val node     = CandidateNode(node1, 10, 10, Some(node1), Set(node1))
    val logState = LogState(100, Some(10), 0)

    val expectedState = LeaderNode(
      node1,
      10,
      sentLength = Map(node2 -> 0, node3 -> 0),
      ackedLength = Map(node2 -> 100, node3 -> 100)
    )

    val expectedActions =
      List(StoreState, AnnounceLeader(node1), ReplicateLog(node2, 10, 100), ReplicateLog(node3, 10, 100))

    node.onReceive(logState, config, VoteResponse(node2, 10, true)) shouldBe (expectedState, expectedActions)
  }

  it should "turn to a Follower node after receiving an AppendEntries request with higher Term" in {
    val node     = CandidateNode(node1, 10, 10, Some(node1), Set(node1))
    val logState = LogState(100, Some(10))

    val command = new WriteCommand[String] {}
    val request =
      AppendEntries(node2, term = 11, logLength = 100, logTerm = 10, leaderAppliedIndex = 100, List(LogEntry(11, 101, command)))

    val expectedResponse = AppendEntriesResponse(node1, 11, 101, true)
    val expectedState    = FollowerNode(node1, 11, None, Some(node2))

    node
      .onReceive(logState, config, request) shouldBe (expectedState, (expectedResponse, List(StoreState, AnnounceLeader(node2))))
  }

  it should "stays in Candidate state and reject the AppendEntries request after receiving AppendEntries with lower Term" in {
    val node     = CandidateNode(node1, 10, 10, Some(node1), Set(node1))
    val logState = LogState(100, Some(10), 0)

    val command = new WriteCommand[String] {}
    val request =
      AppendEntries(node2, term = 10, logLength = 99, logTerm = 10, leaderAppliedIndex = 99, List(LogEntry(10, 100, command)))

    node.onReceive(logState, config, request) shouldBe (node, (AppendEntriesResponse(node1, 10, 0, false), List.empty))
  }
}
