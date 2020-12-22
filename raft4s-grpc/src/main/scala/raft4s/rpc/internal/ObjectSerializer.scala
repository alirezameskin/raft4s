package raft4s.rpc.internal

import com.google.protobuf.ByteString

import java.io.{ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

private[raft4s] object ObjectSerializer {

  def encode[T](obj: T): ByteString = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    oos.writeObject(obj)
    oos.close

    ByteString.copyFrom(stream.toByteArray)
  }

  def decode[T](byteString: ByteString): T = {

    val ois      = new ObjectInputStream(byteString.newInput)
    val response = ois.readObject().asInstanceOf[T]
    ois.close()

    response

  }
}
