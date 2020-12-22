package raft4s.storage.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.util.Try

class JavaSerializer[T] extends Serializer[T] {

  override def toBytes(obj: T): Array[Byte] = {

    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    oos.writeObject(obj)
    oos.close

    stream.toByteArray
  }

  override def fromBytes(bytes: Array[Byte]): Option[T] =
    Try {
      val as       = new ByteArrayInputStream(bytes)
      val ois      = new ObjectInputStream(as)
      val response = ois.readObject().asInstanceOf[T]
      ois.close()

      response
    }.toOption
}
