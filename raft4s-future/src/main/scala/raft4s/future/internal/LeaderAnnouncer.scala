package raft4s.future.internal

import raft4s.Node

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}

private[future] class LeaderAnnouncer(promise: Promise[Node]) (implicit EC: ExecutionContext) extends raft4s.internal.LeaderAnnouncer[Future] {
  private val promiseRef = new AtomicReference[Promise[Node]](promise)

  override def announce(leader: Node): Future[Unit] =
    Future {
      promiseRef.get().trySuccess(leader)
    }

  override def reset(): Future[Unit] =
    Future {
      promiseRef.set(Promise[Node])
    }

  override def listen(): Future[Node] =
    promiseRef.get.future
}

object LeaderAnnouncer {

  def build(implicit EC: ExecutionContext): LeaderAnnouncer =
    new LeaderAnnouncer(Promise[Node])
}
