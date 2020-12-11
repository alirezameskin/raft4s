package raft4s.demo

import cats.effect.IO
import raft4s.StateMachine
import raft4s.protocol.{ReadCommand, WriteCommand}
import raft4s.storage.Snapshot

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import scala.collection.concurrent.TrieMap

case class Put(key: String, value: String) extends WriteCommand[String]
case class Get(key: String)                extends ReadCommand[String]

class KvStateMachine extends StateMachine[IO] {
  private var map             = new TrieMap[String, String]()
  private var lastIndex: Long = -1

  override def applyWrite: PartialFunction[(Long, WriteCommand[_]), IO[Any]] = { case (index, Put(key, value)) =>
    IO {
      map.put(key, value)
      lastIndex = index
      value
    }
  }

  override def applyRead: PartialFunction[ReadCommand[_], IO[Any]] = { case Get(key) =>
    IO.pure(map.get(key))
  }

  override def appliedIndex: IO[Long] = IO(lastIndex)

  override def takeSnapshot(): IO[Snapshot] = IO {
    val copy = map.snapshot()

    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    oos.writeObject(copy)
    oos.close

    Snapshot(lastIndex, 0L, ByteBuffer.wrap(stream.toByteArray))
  }

  override def restoreSnapshot(snapshot: Snapshot): IO[Unit] = IO {
    val ois      = new ObjectInputStream(new ByteArrayInputStream(snapshot.bytes.array()))
    val response = ois.readObject().asInstanceOf[TrieMap[String, String]]
    ois.close()

    map = response
    lastIndex = snapshot.lastIndex
  }
}
