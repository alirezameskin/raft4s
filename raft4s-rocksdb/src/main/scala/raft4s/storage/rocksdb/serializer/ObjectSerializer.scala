package raft4s.storage.rocksdb.serializer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

private[rocksdb] object ObjectSerializer {

  def encode[T](obj: T): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    oos.writeObject(obj)
    oos.close

    stream.toByteArray
  }

  def decode[T](bytes: Array[Byte]): T = {

    val ois      = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val response = ois.readObject().asInstanceOf[T]
    ois.close()

    response

  }

}
