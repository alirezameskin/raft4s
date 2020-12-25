package raft4s.future.internal.impl

import raft4s.Node
import raft4s.internal.LeaderAnnouncer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}

private[future] class LeaderAnnouncerImpl(promise: Promise[Node])(implicit EC: ExecutionContext) extends LeaderAnnouncer[Future] {

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

object LeaderAnnouncerImpl {

  def build(implicit EC: ExecutionContext): LeaderAnnouncerImpl =
    new LeaderAnnouncerImpl(Promise[Node])
}
