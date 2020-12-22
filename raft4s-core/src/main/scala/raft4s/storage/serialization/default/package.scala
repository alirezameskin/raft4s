package raft4s.storage.serialization

import raft4s.LogEntry

import java.nio.ByteBuffer
import scala.util.Try

package object default {

  implicit val longSerializer = new Serializer[Long] {
    override def toBytes(obj: Long): Array[Byte] = {
      val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
      buffer.putLong(obj)
      buffer.array()
    }

    override def fromBytes(bytes: Array[Byte]): Option[Long] =
      Try {
        val buffer = ByteBuffer.allocate(java.lang.Long.BYTES);
        buffer.put(bytes)
        buffer.flip()
        buffer.getLong()
      }.toOption
  }

  implicit val logEntrySerializer       = new JavaSerializer[LogEntry]
  implicit val persistedStateSerializer = new PersistedStateSerializer
}
