package raft4s.demo.grpc

import java.util.concurrent.TimeUnit

import cats.effect.{ExitCode, IO, IOApp}
import raft4s.demo.{Get, KvStateMachine, Put}
import raft4s.storage.memory.MemoryStorage
import raft4s.{Address, Configuration, Raft}

import scala.concurrent.duration.FiniteDuration

object Test extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val config1 =
      Configuration(
        Address("localhost", 9080),
        List(Address("localhost", 9081), Address("localhost", 9082)),
        FiniteDuration(10, TimeUnit.SECONDS)
      )

    import raft4s.rpc.grpc.io.implicits._

    val result1 = for {
      node <- Raft.make[IO](config1, MemoryStorage.empty[IO], new KvStateMachine())
      _    <- node.start()

      s <- node.state.get
      _ = println("Node 1", s)

      res <- node.onCommand(Get("name"))
      _ = println(s"Result in node 1 ${res}")
    } yield ()

    val config2 =
      Configuration(
        Address("localhost", 9081),
        List(Address("localhost", 9080), Address("localhost", 9082)),
        FiniteDuration(0, TimeUnit.SECONDS)
      )
    val result2 = for {
      node <- Raft.make[IO](config2, MemoryStorage.empty[IO], new KvStateMachine())
      _    <- node.start()

      s <- node.state.get
      _ = println("Node 2", s)

      res <- node.onCommand(Put("name", "Alireza"))
      _ = println(res)
    } yield ()

    val config3 =
      Configuration(
        Address("localhost", 9082),
        List(Address("localhost", 9080), Address("localhost", 9081)),
        FiniteDuration(5, TimeUnit.SECONDS)
      )
    val result3 = for {
      node <- Raft.make[IO](config3, MemoryStorage.empty[IO], new KvStateMachine())
      _    <- node.start()

      s <- node.state.get
      _ = println("Node 3", s)

      res <- node.onCommand(Get("name"))
      _ = println(s"Result in node 3 ${res}")
    } yield ()

    for {
      f1 <- result1.start
      f2 <- result2.start
      f3 <- result3.start

      _ <- f1.join
      _ <- f2.join
      - <- f3.join
    } yield ExitCode.Success
  }
}
