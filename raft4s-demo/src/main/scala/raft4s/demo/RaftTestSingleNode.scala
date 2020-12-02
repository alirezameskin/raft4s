package raft4s.demo

import java.util.concurrent.TimeUnit

import cats.effect.{ContextShift, IO, Timer}
import raft4s._
import raft4s.rpc._
import raft4s.storage.memory.MemoryStorage

import scala.concurrent.duration.FiniteDuration

object RaftTestSingleNode extends App {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)
  implicit val timer: Timer[IO]               = IO.timer(scala.concurrent.ExecutionContext.global)

  implicit val clientBuilder = new RpcClientBuilder[IO] {
    override def build(address: Address): RpcClient[IO] = ???
  }

  implicit val serverBuilder = new RpcServerBuilder[IO] {
    override def build(address: Address, raft: Raft[IO]): IO[RpcServer[IO]] = IO(new RpcServer[IO] {
      override def start(): IO[Unit] = IO.unit
    })
  }

  val config = Configuration(Address("node1", 8090), List.empty, FiniteDuration(0, TimeUnit.SECONDS))

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
