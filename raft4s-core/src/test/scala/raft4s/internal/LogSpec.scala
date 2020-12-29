package raft4s.internal

import cats.implicits._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import raft4s.protocol.NewClusterConfiguration
import raft4s.storage.{MemoryLogStorage, MemorySnapshotStorage, Snapshot}
import raft4s.{GetCommand, LogCompactionPolicy, Node, SimpleStateMachine}

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import scala.collection.concurrent.TrieMap
import scala.util.{Success, Try}

class LogSpec extends AnyFlatSpec with should.Matchers {
  implicit val logger: Logger[Try] = new NullLogger[Try]
  val config                       = NewClusterConfiguration(Set(Node("localhost", 1080), Node("localhost", 1081), Node("localhost", 1082)))

  "Log's initialize method" should "install the latest Snapshot and update lastCommitIndex" in {

    val lastSnapshot = Snapshot(10, toBytes(TrieMap.from(Map("name" -> "John", "lastname" -> "Smith"))), config)

    val logStorage        = new MemoryLogStorage[Try](TrieMap.empty)
    val snapshotStorage   = new MemorySnapshotStorage[Try](Some(lastSnapshot))
    val stateMachine      = new SimpleStateMachine[Try]
    val membershipManager = new SimpleMembershipManager[Try](config)

    val log =
      new SimpleLog[Try](logStorage, snapshotStorage, stateMachine, membershipManager, LogCompactionPolicy.noCompaction[Try], 0L)

    log.initialize()

    log.lastCommit shouldBe 10L
    stateMachine.appliedIndex shouldBe Success(10L)
    stateMachine.applyRead(GetCommand("name")) shouldBe Success("John")
  }

  def toBytes(map: TrieMap[String, String]) = {

    val copy = map.snapshot()

    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    oos.writeObject(copy)
    oos.close

    ByteBuffer.wrap(stream.toByteArray)
  }
}
