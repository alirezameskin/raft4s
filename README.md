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
  "com.github.alirezameskin" %% "raft4s-core"         % "0.0.3",
  "com.github.alirezameskin" %% "raft4s-grpc"         % "0.0.3"
)
```

#### Defining a StateMachine and Commands

```scala

import cats.effect.{ExitCode, IO, IOApp, Resource}
import io.odin._
import raft4s.protocol.{ReadCommand, WriteCommand}
import raft4s.rpc.grpc.io.implicits._
import raft4s.storage.memory.MemoryStorage
import raft4s.{Address, Configuration, RaftCluster}

case class SetCommand(key: String, value: String) extends WriteCommand[String]
case class DeleteCommand(key: String)             extends WriteCommand[Unit]
case class GetCommand(key: String)                extends ReadCommand[String]

class KvStateMachine(lastIndex: Ref[IO, Long], map: Ref[IO, Map[String, String]]) extends StateMachine[IO] {

  override def applyWrite: PartialFunction[(Long, WriteCommand[_]), IO[Any]] = {
    case (index, SetCommand(key, value)) =>
      for {
        _ <- map.update(_ + (key -> value))
        _ <- lastIndex.set(index)
      } yield value

    case (index, DeleteCommand(key)) =>
      for {
        _ <- map.update(_.removed(key))
        _ <- lastIndex.set(index)
      } yield ()
  }

  override def applyRead: PartialFunction[ReadCommand[_], IO[Any]] = { case GetCommand(key) =>
    for {
      items <- map.get
      _ = println(items)
    } yield items(key)
  }

  override def appliedIndex: IO[Long] = lastIndex.get

  override def takeSnapshot(): IO[Snapshot] =
    for {
      items <- map.get
      index <- lastIndex.get
      bytes = serialize(items)
    } yield Snapshot(index, bytes)

  override def restoreSnapshot(snapshot: Snapshot): IO[Unit] =
    for {
      _ <- map.set(deserialize(snapshot.bytes))
      _ <- lastIndex.set(snapshot.lastIndex)
    } yield ()

  private def serialize(items: Map[String, String]): ByteBuffer = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    oos.writeObject(items)
    oos.close

    ByteBuffer.wrap(stream.toByteArray)
  }

  private def deserialize(bytes: ByteBuffer): Map[String, String] = {

    val ois      = new ObjectInputStream(new ByteArrayInputStream(bytes.array()))
    val response = ois.readObject().asInstanceOf[Map[String, String]]
    ois.close()

    response
  }
}

object KvStateMachine {
  def empty: IO[KvStateMachine] =
    for {
      index <- Ref.of[IO, Long](-1L)
      map   <- Ref.of[IO, Map[String, String]](Map.empty)
    } yield new KvStateMachine(index, map)
}
```

#### Creating the cluster

```scala
import cats.effect.{ExitCode, IO, IOApp, Resource}
import io.odin._
import raft4s.demo.kvstore.command.{GetCommand, SetCommand}
import raft4s.rpc.grpc.io.implicits._
import raft4s.storage.memory.MemoryStorage
import raft4s.{Address, Configuration, RaftCluster}

object SampleKVApp extends IOApp {

  val config = Configuration(Address("localhost", 8090), List(Address("localhost", 8091), Address("localhost", 8092)))

  implicit val logger: Logger[IO] = consoleLogger()

  override def run(args: List[String]): IO[ExitCode] =
    makeCluster(config).use { cluster =>
      for {
        leader <- cluster.start
        _      <- IO(println(s"Election is completed. Leader is ${leader}"))
        _      <- cluster.execute(SetCommand("key", "value"))
        _      <- IO(println("Set command is executed"))
        result <- cluster.execute(GetCommand("key"))
        _      <- IO(println(s"Result of the get command is : ${result}"))
      } yield ExitCode.Success
    }

  private def makeCluster(config: Configuration): Resource[IO, RaftCluster[IO]] =
    for {
      stateMachine <- Resource.liftF(KvStateMachine.empty)
      storage      <- Resource.liftF(MemoryStorage.empty[IO])
      cluster      <- RaftCluster.resource(config, storage, stateMachine)
    } yield cluster
}
```
