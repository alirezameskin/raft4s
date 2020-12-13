package raft4s.demo

import cats.effect.{ContextShift, IO, Resource, Timer}
import io.odin
import io.odin.Logger
import raft4s.protocol._
import raft4s.rpc._
import raft4s.storage.Snapshot
import raft4s.storage.memory.MemoryStorage
import raft4s.{Address, Configuration, Raft}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

object RaftTest extends App {

  implicit val logger: Logger[IO]             = odin.consoleLogger()
  implicit val contextShift: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)
  implicit val timer: Timer[IO]               = IO.timer(scala.concurrent.ExecutionContext.global)

  val clients = scala.collection.mutable.Map.empty[String, RpcClient[IO]]

  implicit val clientBuilder = new RpcClientBuilder[IO] {
    override def build(address: Address): RpcClient[IO] =
      new RpcClient[IO] {
        override def send(voteRequest: VoteRequest): IO[VoteResponse] =
          clients(address.id).send(voteRequest)

        override def send(appendEntries: AppendEntries): IO[AppendEntriesResponse] =
          clients(address.id).send(appendEntries)

        override def send[T](command: Command[T]): IO[T] =
          clients(address.id).send(command)

        override def send(snapshot: Snapshot, lastEntry: LogEntry): IO[AppendEntriesResponse] =
          clients(address.id).send(snapshot, lastEntry)

        override def close(): IO[Unit] = IO.unit
      }
  }

  implicit val serverBuilder = new RpcServerBuilder[IO] {
    override def build(address: Address, raft: Raft[IO]): Resource[IO, RpcServer[IO]] =
      Resource.pure[IO, RpcServer[IO]](new RpcServer[IO] {
        override def start(): IO[Unit] = IO.unit
      })
  }

  val nodes                  = List("node1", "node2", "node3")
  val (node1, stateMachine1) = createNode("node1", nodes)
  val (node2, stateMachine2) = createNode("node2", nodes)
  val (node3, stateMachine3) = createNode("node3", nodes)

  val client1 = createClient(node1, "node1")
  val client2 = createClient(node2, "node2")
  val client3 = createClient(node3, "node3")

  clients.put("node1:8090", client1)
  clients.put("node2:8090", client2)
  clients.put("node3:8090", client3)

  val result = for {
    _      <- node2.start()
    _      <- node3.start()
    leader <- node1.start()

    _ = println("Leader is : " + leader)

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

    _ <- Timer[IO].sleep(FiniteDuration(2, TimeUnit.SECONDS))

  } yield ()

  result.unsafeRunSync()

  scala.Predef.ensuring(stateMachine2.applyRead(Get("name")).unsafeRunSync() == "Alireza")
  scala.Predef.ensuring(stateMachine3.applyRead(Get("name")).unsafeRunSync() == "Alireza")

  scala.Predef.ensuring(stateMachine2.applyRead(Get("lastname")).unsafeRunSync() == "Meskin")
  scala.Predef.ensuring(stateMachine3.applyRead(Get("lastname")).unsafeRunSync() == "Meskin")

  def createNode(nodeId: String, nodes: List[String]): (Raft[IO], KvStateMachine) = {
    val configuration = Configuration(
      local = Address(nodeId, 8090),
      members = nodes.map(id => Address(id, 8090)),
      followerAcceptRead = false
    )

    val stateMachine = new KvStateMachine()
    val node = MemoryStorage.empty[IO].flatMap { storage =>
      Raft.make[IO](configuration, storage, stateMachine)
    }
    (node.unsafeRunSync(), stateMachine)
  }

  def createClient(node: Raft[IO], nodeId: String): RpcClient[IO] =
    new RpcClient[IO] {
      override def send(voteRequest: VoteRequest): IO[VoteResponse] =
        IO(println(s"vote request received ${voteRequest} on node ${nodeId}")) *> node.onReceive(voteRequest)

      override def send(appendEntries: AppendEntries): IO[AppendEntriesResponse] =
        IO(println(s"Append entries request received ${appendEntries} on node ${nodeId}")) *> node.onReceive(
          appendEntries
        )

      override def send[T](command: Command[T]): IO[T] =
        IO(println(s"Sending a command to node ${nodeId}")) *> node.onCommand(command)

      override def send(snapshot: Snapshot, lastEntry: LogEntry): IO[AppendEntriesResponse] =
        IO(println(s"Sending an snapshot to node ${nodeId}")) *> node.onReceive(
          InstallSnapshot(snapshot, lastEntry)
        )

      override def close(): IO[Unit] = IO.unit
    }
}
