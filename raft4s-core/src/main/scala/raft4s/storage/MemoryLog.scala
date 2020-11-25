package raft4s.storage

import cats.effect.IO
import raft4s.log.{Log, LogEntry}

import scala.collection.concurrent.TrieMap

class MemoryLog extends Log{
  val map = TrieMap[Long, LogEntry]()

  var _commitIndex: Long = 0

  def length: IO[Long] = IO(map.size)

  def get(index: Long): IO[LogEntry] = IO { map.get(index).orNull }

  override def commitLength: IO[Long] = IO {_commitIndex }

  override def updateCommitLength(index: Long): IO[Unit] = IO { _commitIndex = index }

  override def put(index: Long, logEntry: LogEntry): IO[LogEntry] = IO {
    map.put(index, logEntry)
    logEntry
  }
}
