## raft4s

An implementation of [the Raft distributed consensus](https://raft.github.io/) algorithm written in Scala with [Cats library](https://typelevel.org/cats-effect/).

### Implemented features
* Leader Election
* Log replication

### Usage
Complete example implementation can be found on [https://github.com/alirezameskin/raft4s-kvstore-example](https://github.com/alirezameskin/raft4s-kvstore-example)

#### SBT settings

```sbt
resolvers += "raft4s".at("https://maven.pkg.github.com/alirezameskin/raft4s")

libraryDependencies ++= Seq(
  "com.github.alirezameskin" %% "raft4s-core"         % "0.0.1",
  "com.github.alirezameskin" %% "raft4s-grpc"         % "0.0.1"
)
```

#### Defining a StateMachine and Commands

```scala
import cats.effect.IO
import raft4s.StateMachine
import raft4s.demo.kvstore.command.{DeleteCommand, GetCommand, SetCommand}
import raft4s.protocol.{ReadCommand, WriteCommand}

import java.util.HashMap

case class SetCommand(key: String, value: String) extends WriteCommand[String]
case class DeleteCommand(key: String)             extends WriteCommand[Unit]
case class GetCommand(key: String)                extends ReadCommand[String]


class KvStateMachine extends StateMachine[IO] {
  private var map             = new HashMap[String, String]()
  private var lastIndex: Long = 0

  override def applyWrite: PartialFunction[(Long, WriteCommand[_]), IO[Any]] = {
    case (index, SetCommand(key, value)) =>
      IO {
        map.put(key, value)
        lastIndex = index
        value
      }
    case (index, DeleteCommand(key)) =>
      IO {
        map.remove(key)
        lastIndex = index
        ()
      }
  }

  override def applyRead: PartialFunction[ReadCommand[_], IO[Any]] = {
    case GetCommand(key) =>
      IO.pure(map.get(key))
  }
}
```

#### Creating the cluster

```scala
import cats.effect.{ExitCode, IO, IOApp}
import raft4s._
import raft4s.demo.kvstore.command.{GetCommand, SetCommand}
import raft4s.storage.memory.MemoryStorage
import raft4s.rpc.grpc.io.implicits._

object Example extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    for {
      config  <- IO.pure(Configuration(Address("localhost", 8090), List(Address("localhost", 8091), Address("localhost", 8092))))
      cluster <- Raft.make[IO](config, MemoryStorage.empty[IO], new KvStateMachine())
      leader  <- cluster.start()
      _       <- IO(println(s"Election is completed. Leader is ${leader}"))
      _       <- cluster.onCommand(SetCommand("key", "value"))
      _       <- IO(println("Set command is executed"))
      result  <- cluster.onCommand(GetCommand("key"))
      _       <- IO(println(s"Result of the get command is : ${result}"))
    } yield ExitCode.Success
}

```
