package raft4s.demo

import cats.effect.{ContextShift, IO, Timer}
import raft4s._
import raft4s.rpc._
import raft4s.storage.memory.MemoryStorage

object RaftTestSingleNode extends App {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)
  implicit val timer: Timer[IO]               = IO.timer(scala.concurrent.ExecutionContext.global)

  implicit val clientBuilder = new RpcClientBuilder[IO] {
    override def build(server: Server): RpcClient[IO] = ???
  }

  val config = Configuration(Server("node1", Address("localhost", 8090)), List.empty)

  val result = for {
    node <- Raft.make[IO](config, MemoryStorage.empty[IO], new KvStateMachine())
    _    <- node.start()

    s <- node.state.get
    _ = println("Node 1", s)
    _ = println("Sending a new command")

    res <- node.onCommand(Put("name", "Reza"))
    _ = println(s"Command output : ${res}")

    res <- node.onCommand(Put("lastname", "Meskin"))
    _ = println(s"Command output : ${res}")

    res <- node.onCommand(Get("name"))
    _ = println(s"Command output : ${res}")

    res <- node.onCommand(Put("name", "Alireza"))
    _ = println(s"Command output : ${res}")

    res <- node.onCommand(Get("name"))
    _ = println(s"Command output : ${res}")
  } yield ()

  result.unsafeRunSync()
}
