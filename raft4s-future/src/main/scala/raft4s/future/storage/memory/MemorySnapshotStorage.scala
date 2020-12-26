package raft4s.future.storage.memory

import raft4s.storage.{Snapshot, SnapshotStorage}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class MemorySnapshotStorage(implicit EC: ExecutionContext) extends SnapshotStorage[Future] {

  private val ref = new AtomicReference[Option[Snapshot]](None)

  override def saveSnapshot(snapshot: Snapshot): Future[Unit] =
    Future {
      ref.set(Some(snapshot))
    }

  override def retrieveSnapshot(): Future[Option[Snapshot]] =
    Future {
      ref.get
    }

  override def getLatestSnapshot(): Future[Option[Snapshot]] =
    Future {
      ref.get
    }
}

object MemorySnapshotStorage {
  def empty(implicit EC: ExecutionContext): MemorySnapshotStorage =
    new MemorySnapshotStorage()
}
