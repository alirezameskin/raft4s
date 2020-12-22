package raft4s.node

import org.scalatest.flatspec._
import org.scalatest.matchers._
import raft4s.{LogEntry, Node, WriteCommand}
import raft4s.protocol.{LogState, _}

class LeaderNodeSpec extends AnyFlatSpec with should.Matchers {
  val node1  = Node("node1", 1080)
  val node2  = Node("node2", 1080)
  val node3  = Node("node3", 1080)
  val nodes  = Set(node1, node2, node3)
  val config = NewClusterConfiguration(nodes)

  "A Leader node" should "do nothing after getting an Election timer" in {
    val node     = LeaderNode(node1, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10))

    node.onTimer(logState, config) shouldBe (node, List.empty)
  }

  it should "not change any state and not produce any when receives a Vote response" in {
    val node     = LeaderNode(node1, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10))

    node.onReceive(logState, config, VoteResponse(node3, 10, true)) shouldBe (node, List.empty)
  }

  it should "reject votes with lower Term" in {
    val node     = LeaderNode(node1, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10))

    val expectedNode    = node.copy(matchIndex = Map(node3 -> 100), nextIndex = Map(node3 -> 101))
    val expectedActions = List(ReplicateLog(node3, 10, 101))
    val expectedResult  = (expectedNode, (VoteResponse(node1, 10, false), expectedActions))

    node.onReceive(logState, config, VoteRequest(node3, 9, 100, 9)) shouldBe expectedResult
  }

  it should "turn to a Follower node when it gets an Vote with higher Term" in {
    val node     = LeaderNode(node1, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10))

    val expectedNode     = FollowerNode(node1, 12, Some(node3))
    val expectedResponse = (VoteResponse(node1, 12, true), List(StoreState, ResetLeaderAnnouncer))

    node.onReceive(logState, config, VoteRequest(node3, 12, 100, 10)) shouldBe (expectedNode, expectedResponse)
  }

  it should "not change its change when gets AppendEntries with lower Term" in {
    val node     = LeaderNode(node1, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10))
    val prevLog  = Some(LogEntry(10, 100, new WriteCommand[String] {}))

    val expectedNode = node
    val request      = AppendEntries(node2, 9, 99, 9, 99, List(LogEntry(9, 100, new WriteCommand[String] {})))

    node.onReceive(logState, config, request, prevLog) shouldBe (expectedNode, (
      AppendEntriesResponse(node1, 10, 99, false),
      List.empty
    ))
  }

  it should "turn to a Follower node when gets AppendEntries with higher Term" in {
    val node     = LeaderNode(node1, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10), 0)

    val expectedNode = FollowerNode(node1, 11, None, Some(node2))
    val request      = AppendEntries(node2, 11, 100, 10, 100, List(LogEntry(11, 101, new WriteCommand[String] {})))
    val prevLog      = Some(LogEntry(10, 100, new WriteCommand[String] {}))

    node.onReceive(logState, config, request, prevLog) shouldBe (expectedNode, (
      AppendEntriesResponse(node1, 11, 101, true),
      List(StoreState, AnnounceLeader(node2, true))
    ))
  }

  it should "turn to a Follower node when gets AppendEntriesResponse with higher Term" in {

    val node     = LeaderNode(node1, 10, Map.empty, Map.empty)
    val logState = LogState(100, Some(10))

    val expectedNode = FollowerNode(node1, 11)
    val request      = AppendEntriesResponse(node2, 11, 1, true)

    node.onReceive(logState, config, request) shouldBe (expectedNode, List(StoreState, ResetLeaderAnnouncer))
  }

  it should "commit logs when it gets a success response for AppendEntries" in {
    val node     = LeaderNode(node1, 10, Map(node2 -> 100, node3 -> 100), Map(node2 -> 0, node3 -> 0))
    val logState = LogState(101, Some(10))

    val expectedNode    = LeaderNode(node1, 10, matchIndex = Map(node2 -> 101, node3 -> 100), nextIndex = Map(node2 -> 102, node3 -> 0))
    val expectedActions = List(CommitLogs(Map(node2 -> 101, node3 -> 100, node1 -> 101)))

    node.onReceive(logState, config, AppendEntriesResponse(node2, 10, 101, true)) shouldBe (expectedNode, expectedActions)
  }

  it should "replicate logs when it gets a unsuccessful response for AppendEntries" in {
    val node     = LeaderNode(node1, 10, Map(node2 -> 100, node3 -> 100), Map(node2 -> 100, node3 -> 100))
    val logState = LogState(101, Some(10))

    val expectedNode    = LeaderNode(node1, 10, Map(node2 -> 100, node3 -> 100), Map(node2 -> 99, node3 -> 100))
    val expectedActions = List(ReplicateLog(node2, 10, 99))

    node.onReceive(logState, config, AppendEntriesResponse(node2, 10, 101, false)) shouldBe (expectedNode, expectedActions)
  }
}
