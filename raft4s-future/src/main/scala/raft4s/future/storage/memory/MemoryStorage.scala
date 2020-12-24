package raft4s.future.storage.memory

import raft4s.Storage

import scala.concurrent.{ExecutionContext, Future}

object MemoryStorage {
  def empty(implicit EC: ExecutionContext): Storage[Future] =
    new raft4s.Storage[Future](MemoryLogStorage.empty, MemoryStateStorage.empty, MemorySnapshotStorage.empty)

}
