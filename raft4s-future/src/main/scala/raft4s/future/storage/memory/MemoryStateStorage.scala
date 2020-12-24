package raft4s.future.storage.memory

import raft4s.storage.{PersistedState, StateStorage}

import scala.concurrent.{ExecutionContext, Future}

class MemoryStateStorage(implicit EC: ExecutionContext) extends StateStorage[Future] {

  override def persistState(state: PersistedState): Future[Unit] =
    Future.successful(())

  override def retrieveState(): Future[Option[PersistedState]] =
    Future.successful(Some(PersistedState(0, None, 0L)))
}

object MemoryStateStorage {
  def empty(implicit EC: ExecutionContext): StateStorage[Future] =
    new MemoryStateStorage
}
