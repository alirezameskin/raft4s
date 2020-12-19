package raft4s.storage

import raft4s.protocol.ClusterConfiguration

import java.nio.ByteBuffer

case class Snapshot(lastIndex: Long, bytes: ByteBuffer, config: ClusterConfiguration)
