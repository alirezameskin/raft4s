package raft4s

import cats.Monad

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import scala.collection.concurrent.TrieMap

class SimpleStateMachine[F[_]: Monad] extends StateMachine[F] {

  private var map             = new TrieMap[String, String]()
  private var lastIndex: Long = 0

  override def applyWrite: PartialFunction[(Long, WriteCommand[_]), F[Any]] = { case (index, PutCommand(key, value)) =>
    Monad[F].pure {
      map.put(key, value)
      lastIndex = index
      value
    }
  }

  override def applyRead: PartialFunction[ReadCommand[_], F[Any]] = { case GetCommand(key) =>
    Monad[F].pure(map.get(key).orNull)
  }

  override def appliedIndex: F[Long] =
    Monad[F].pure(lastIndex)

  override def takeSnapshot(): F[(Long, ByteBuffer)] =
    Monad[F].pure {
      val copy = map.snapshot()

      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos                           = new ObjectOutputStream(stream)
      oos.writeObject(copy)
      oos.close

      (lastIndex, ByteBuffer.wrap(stream.toByteArray))
    }

  override def restoreSnapshot(index: Long, bytes: ByteBuffer): F[Unit] =
    Monad[F].pure {
      val ois      = new ObjectInputStream(new ByteArrayInputStream(bytes.array()))
      val response = ois.readObject().asInstanceOf[TrieMap[String, String]]
      ois.close()

      map = response
      lastIndex = index
    }
}
