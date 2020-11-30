package raft4s.demo

import java.util.HashMap

import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, IO}
import raft4s.log.ReplicatedLog
import raft4s.node.{FollowerNode, NodeState}
import raft4s.rpc._
import raft4s.storage.MemoryLog
import raft4s.{Raft, StateMachine}

case class Put(key: String, value: String) extends WriteCommand[String]
case class Get(key: String)                extends ReadCommand[String]

class KvStateMachine extends StateMachine[IO] {
  private var map             = new HashMap[String, String]()
  private var lastIndex: Long = 0

  override def applyWrite: PartialFunction[(Long, WriteCommand[_]), IO[Any]] = { case (index, Put(key, value)) =>
    map.put(key, value)
    lastIndex = index
    IO.pure(value)
  }

  override def applyRead: PartialFunction[ReadCommand[_], IO[Any]] = { case Get(key) =>
    IO.pure(map.get(key))
  }
}

object RaftTest extends App {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)

  val log2          = MemoryLog.empty[IO]
  val stateMachine2 = new KvStateMachine()
  val rlog2         = new ReplicatedLog(log2, stateMachine2)
  val raftNode2 =
    new Raft("node2", Map.empty, rlog2, Ref.of[IO, NodeState](FollowerNode("node2", nodes, 0, None, None)).unsafeRunSync())
  val raft2Client = new RpcClient[IO] {
    override def send(voteRequest: VoteRequest): IO[VoteResponse] =
      IO(println(s"vote request received ${voteRequest} on node 2")) *> raftNode2.onReceive(voteRequest)

    override def send(appendEntries: AppendEntries): IO[AppendEntriesResponse] =
      IO(println(s"Append entries request received ${appendEntries} on node 2")) *> raftNode2.onReceive(appendEntries)
  }

  val log3          = MemoryLog.empty[IO]
  val stateMachine3 = new KvStateMachine()
  val rlog3         = new ReplicatedLog(log3, stateMachine3)
  val raftNode3 =
    new Raft("node3", Map.empty, rlog3, Ref.of[IO, NodeState](FollowerNode("node3", nodes, 0, None, None)).unsafeRunSync())
  val raft3Client = new RpcClient[IO] {
    override def send(voteRequest: VoteRequest): IO[VoteResponse] =
      IO(println(s"vote request received ${voteRequest} on node 3")) *> raftNode3.onReceive(voteRequest)

    override def send(appendEntries: AppendEntries): IO[AppendEntriesResponse] =
      IO(println(s"Append entries request received ${appendEntries} on node 3")) *> raftNode3.onReceive(appendEntries)
  }

  val nodes         = List("node1", "node2", "node3")
  val members       = Map("node2" -> raft2Client, "node3" -> raft3Client)
  val leaderLog     = MemoryLog.empty[IO]
  val stateMachine1 = new KvStateMachine()
  val rLeaderLog    = new ReplicatedLog(leaderLog, stateMachine1)
  val result = for {
    state <- Ref.of[IO, NodeState](FollowerNode("node1", nodes, 0, None, None))
    raft = new Raft("node1", members, rLeaderLog, state)
    _ <- raft.start()
    s <- raft.state.get
    _ = println("Node 1", s)
    _ = println("Sending a new command")
    res <- raft.onCommand(Put("name", "Reza"))
    _ = println(s"Command output : ${res}")

    res <- raft.onCommand(Put("lastname", "Meskin"))
    _ = println(s"Command output : ${res}")

    res <- raft.onCommand(Get("name"))
    _ = println(s"Command output : ${res}")

    res <- raft.onCommand(Put("name", "Alireza"))
    _ = println(s"Command output : ${res}")

    res <- raft.onCommand(Get("name"))
    _ = println(s"Command output : ${res}")
  } yield ()

  result.unsafeRunSync()

  println(log2.map)
  println(log3.map)

  scala.Predef.ensuring(stateMachine2.applyRead(Get("name")).unsafeRunSync() == "Alireza")
  scala.Predef.ensuring(stateMachine3.applyRead(Get("name")).unsafeRunSync() == "Alireza")

  scala.Predef.ensuring(stateMachine2.applyRead(Get("lastname")).unsafeRunSync() == "Meskin")
  scala.Predef.ensuring(stateMachine3.applyRead(Get("lastname")).unsafeRunSync() == "Meskin")

}
