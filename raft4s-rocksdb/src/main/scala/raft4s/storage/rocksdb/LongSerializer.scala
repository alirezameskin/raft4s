package raft4s.storage.rocksdb

import java.nio.ByteBuffer

private[rocksdb] object LongSerializer {
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
