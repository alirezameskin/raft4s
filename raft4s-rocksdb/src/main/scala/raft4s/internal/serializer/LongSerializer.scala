package raft4s.internal.serializer

import java.nio.ByteBuffer

private[raft4s] object LongSerializer {
  def toBytes(long: Long): Array[Byte] = {
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.putLong(long)
    buffer.array()
  }

  def toLong(bytes: Array[Byte]): Long = {
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES);
    buffer.put(bytes)
    buffer.flip()
    buffer.getLong()
  }

}
