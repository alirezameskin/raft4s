package raft4s.future.storage.memory

import raft4s.LogEntry
import raft4s.storage.LogStorage

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.{ExecutionContext, Future}

class MemoryLogStorage(implicit EC: ExecutionContext) extends LogStorage[Future] {

  private val lastIndexRef = new AtomicLong(0L)
  private val itemsRef     = new AtomicReference[Map[Long, LogEntry]]()

  override def lastIndex: Future[Long] =
    Future {
      lastIndexRef.get
    }

  override def get(index: Long): Future[LogEntry] =
    Future {
      val map = itemsRef.get
      map.get(index).orNull
    }

  override def put(index: Long, logEntry: LogEntry): Future[LogEntry] =
    Future {
      itemsRef.getAndUpdate(items => items + (index -> logEntry))
      lastIndexRef.getAndUpdate(i => Math.max(i, index))

      logEntry
    }

  override def deleteBefore(index: Long): Future[Unit] =
    Future {
      itemsRef.getAndUpdate(items => items.filter(_._1 >= index))
    }

  override def deleteAfter(index: Long): Future[Unit] =
    Future {
      itemsRef.getAndUpdate(items => items.filter(_._1 <= index))
    }
}

object MemoryLogStorage {
  def empty(implicit EC: ExecutionContext): LogStorage[Future] =
    new MemoryLogStorage
}
