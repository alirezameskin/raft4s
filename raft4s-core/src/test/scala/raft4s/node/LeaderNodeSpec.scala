package raft4s.node

import org.scalatest.flatspec._
import org.scalatest.matchers._
import raft4s.{AnnounceLeader, CommitLogs, ReplicateLog}
import raft4s.log.LogState
import raft4s.protocol.{AppendEntries, AppendEntriesResponse, LogEntry, VoteRequest, VoteResponse, WriteCommand}

class LeaderNodeSpec extends AnyFlatSpec with should.Matchers {
  val nodeId = "node1"
  val nodes  = List("node1", "node2", "node3")

  "A Leader node" should "do nothing after getting an Election timer" in {
    val node     = LeaderNode(nodeId, nodes, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10))

    node.onTimer(logState) shouldBe (node, List.empty)
  }

  it should "not change any state and not produce any when receives a Vote response" in {
    val node     = LeaderNode(nodeId, nodes, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10))

    node.onReceive(logState, VoteResponse("node3", 10, true)) shouldBe (node, List.empty)
  }

  it should "reject votes with lower Term" in {
    val node     = LeaderNode(nodeId, nodes, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10))

    node.onReceive(logState, VoteRequest("node3", 9, 100, 9)) shouldBe (node, VoteResponse("node1", 9, false))
  }

  it should "turn to a Follower node when it gets an Vote with higher Term" in {
    val node     = LeaderNode(nodeId, nodes, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10))

    val expectedNode     = FollowerNode(nodeId, nodes, 12, Some("node3"))
    val expectedResponse = VoteResponse(nodeId, 12, true)

    node.onReceive(logState, VoteRequest("node3", 12, 100, 10)) shouldBe (expectedNode, expectedResponse)
  }

  it should "not change its change when gets AppendEntries with lower Term" in {
    val node     = LeaderNode(nodeId, nodes, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10))

    val expectedNode = node
    val request      = AppendEntries("node2", 9, 99, 9, 99, List(LogEntry(9, 100, new WriteCommand[String] {})))

    node.onReceive(logState, request) shouldBe (expectedNode, (AppendEntriesResponse(nodeId, 10, 0, false), List.empty))
  }

  it should "turn to a Follower node when gets AppendEntries with higher Term" in {
    val node     = LeaderNode(nodeId, nodes, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10))

    val expectedNode = FollowerNode(nodeId, nodes, 11, None, Some("node2"))
    val request      = AppendEntries("node2", 11, 100, 10, 100, List(LogEntry(11, 101, new WriteCommand[String] {})))

    node.onReceive(logState, request) shouldBe (expectedNode, (
      AppendEntriesResponse(nodeId, 11, 101, true),
      List(AnnounceLeader("node2"))
    ))
  }

  it should "turn to a Follower node when gets AppendEntriesResponse with higher Term" in {

    val node     = LeaderNode(nodeId, nodes, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10))

    val expectedNode = FollowerNode(nodeId, nodes, 11)
    val request      = AppendEntriesResponse("node2", 11, 1, true)

    node.onReceive(logState, request) shouldBe (expectedNode, List.empty)
  }

  it should "commit logs when it gets a success response for AppendEntries" in {
    val node     = LeaderNode(nodeId, nodes, 10, Map("node2" -> 100, "node3" -> 100), Map("node2" -> 0, "node3" -> 0))
    val logState = LogState(101, Some(10))

    val expectedNode    = LeaderNode(nodeId, nodes, 10, Map("node2" -> 101, "node3" -> 100), Map("node2" -> 101, "node3" -> 0))
    val expectedActions = List(CommitLogs(Map("node2" -> 101, "node3" -> 100), minAckes = 2))

    node.onReceive(logState, AppendEntriesResponse("node2", 10, 101, true)) shouldBe (expectedNode, expectedActions)
  }

  it should "replicate logs when it gets a unsuccessful response for AppendEntries" in {
    val node     = LeaderNode(nodeId, nodes, 10, Map("node2" -> 100, "node3" -> 100), Map("node2" -> 100, "node3" -> 100))
    val logState = LogState(101, Some(10))

    val expectedNode    = LeaderNode(nodeId, nodes, 10, Map("node2" -> 100, "node3" -> 100), Map("node2" -> 99, "node3" -> 100))
    val expectedActions = List(ReplicateLog("node2", 10, 99), ReplicateLog("node3", 10, 100))

    node.onReceive(logState, AppendEntriesResponse("node2", 10, 101, false)) shouldBe (expectedNode, expectedActions)
  }
}
