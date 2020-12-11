package raft4s.storage

import java.nio.ByteBuffer

case class Snapshot(lastIndex: Long, bytes: ByteBuffer)
