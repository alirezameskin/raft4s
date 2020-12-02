package raft4s.demo

import java.util.concurrent.TimeUnit

import cats.effect.{ContextShift, IO, Timer}
import raft4s.protocol.{AppendEntries, AppendEntriesResponse, VoteRequest, VoteResponse}
import raft4s.rpc._
import raft4s.storage.memory.MemoryStorage
import raft4s.{Address, Configuration, Raft, Server}

import scala.concurrent.duration.FiniteDuration

object RaftTest extends App {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)
  implicit val timer: Timer[IO]               = IO.timer(scala.concurrent.ExecutionContext.global)

  val clients = scala.collection.mutable.Map.empty[String, RpcClient[IO]]

  implicit val clientBuilder = new RpcClientBuilder[IO] {
    override def build(server: Server): RpcClient[IO] = new RpcClient[IO] {
      override def send(voteRequest: VoteRequest): IO[VoteResponse] = clients(server.id).send(voteRequest)

      override def send(appendEntries: AppendEntries): IO[AppendEntriesResponse] = clients(server.id).send(appendEntries)
    }
  }

  val nodes = List("node1", "node2", "node3")
  val node1 = createNode("node1", nodes)
  val node2 = createNode("node2", nodes)
  val node3 = createNode("node3", nodes)

  val client1 = createClient(node1)
  val client2 = createClient(node2)
  val client3 = createClient(node3)

  clients.put("node1", client1)
  clients.put("node2", client2)
  clients.put("node3", client3)

  val result = for {
    _ <- node2.start()
    _ <- node3.start()
    _ <- node1.start()

    s <- node1.state.get
    _ = println("Node 1", s)
    _ = println("Sending a new command")

    res <- node1.onCommand(Put("name", "Reza"))
    _ = println(s"Command output : ${res}")

    _ <- Timer[IO].sleep(FiniteDuration(20, TimeUnit.SECONDS))

    res <- node1.onCommand(Put("lastname", "Meskin"))
    _ = println(s"Command output : ${res}")

    res <- node1.onCommand(Get("name"))
    _ = println(s"Command output : ${res}")

    res <- node1.onCommand(Put("name", "Alireza"))
    _ = println(s"Command output : ${res}")

    res <- node1.onCommand(Get("name"))
    _ = println(s"Command output : ${res}")
  } yield ()

  result.unsafeRunSync()

  scala.Predef.ensuring(node2.log.stateMachine.applyRead(Get("name")).unsafeRunSync() == "Alireza")
  scala.Predef.ensuring(node3.log.stateMachine.applyRead(Get("name")).unsafeRunSync() == "Alireza")

  scala.Predef.ensuring(node2.log.stateMachine.applyRead(Get("lastname")).unsafeRunSync() == "Meskin")
  scala.Predef.ensuring(node2.log.stateMachine.applyRead(Get("lastname")).unsafeRunSync() == "Meskin")

  def createNode(nodeId: String, nodes: List[String]): Raft[IO] = {
    val configuration = Configuration(
      local = Server(nodeId, Address("localhost", 8090)),
      members = nodes.filterNot(_ == nodeId).map(id => Server(id, Address("localhost", 8090)))
    )
    val node = Raft.make[IO](configuration, MemoryStorage.empty[IO], new KvStateMachine())
    node.unsafeRunSync()
  }

  def createClient(node: Raft[IO]): RpcClient[IO] =
    new RpcClient[IO] {
      override def send(voteRequest: VoteRequest): IO[VoteResponse] =
        IO(println(s"vote request received ${voteRequest} on node ${node.nodeId}")) *> node.onReceive(voteRequest)

      override def send(appendEntries: AppendEntries): IO[AppendEntriesResponse] =
        IO(println(s"Append entries request received ${appendEntries} on node ${node.nodeId}")) *> node.onReceive(appendEntries)
    }
}
