package raft4s.demo

import cats.effect.{ContextShift, IO, Resource, Timer}
import io.odin
import io.odin.Logger
import raft4s._
import raft4s.rpc._
import raft4s.storage.memory.MemoryStorage

object RaftTestSingleNode extends App {

  implicit val logger: Logger[IO]             = odin.consoleLogger()
  implicit val contextShift: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)
  implicit val timer: Timer[IO]               = IO.timer(scala.concurrent.ExecutionContext.global)

  implicit val clientBuilder = new RpcClientBuilder[IO] {
    override def build(address: Address): RpcClient[IO] = ???
  }

  implicit val serverBuilder = new RpcServerBuilder[IO] {
    override def resource(address: Address, raft: Raft[IO]): Resource[IO, RpcServer[IO]] =
      Resource.pure[IO, RpcServer[IO]](new RpcServer[IO] {
        override def start(): IO[Unit] = IO.unit
      })
  }

  val config = Configuration(Address("node1", 8090), List.empty)

  val result = for {
    storage <- MemoryStorage.empty[IO]
    node    <- Raft.build[IO](config, storage, new KvStateMachine())
    _       <- node.start()

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
