package raft4s.future.storage.memory

import raft4s.LogEntry
import raft4s.storage.LogStorage

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class MemoryLogStorage(implicit EC: ExecutionContext) extends LogStorage[Future] {

  private val map = mutable.TreeMap.empty[Long, LogEntry]

  override def lastIndex: Future[Long] =
    Future {
      map.size
    }

  override def get(index: Long): Future[LogEntry] =
    Future {
      map.get(index).orNull
    }

  override def put(index: Long, logEntry: LogEntry): Future[LogEntry] =
    Future {
      map.put(index, logEntry)
      logEntry
    }

  override def deleteBefore(index: Long): Future[Unit] =
    Future {
      map.keysIterator.takeWhile(_ < index).foreach(map.remove)
    }

  override def deleteAfter(index: Long): Future[Unit] =
    Future {
      map.keysIterator.withFilter(_ > index).foreach(map.remove)
    }
}

object MemoryLogStorage {
  def empty(implicit EC: ExecutionContext): LogStorage[Future] =
    new MemoryLogStorage
}
