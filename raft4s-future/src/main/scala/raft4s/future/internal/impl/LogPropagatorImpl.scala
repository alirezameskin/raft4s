package raft4s.future.internal.impl

import raft4s.Node
import raft4s.internal.{Log, LogPropagator, Logger, RpcClientProvider}
import raft4s.protocol.AppendEntriesResponse
import raft4s.storage.Snapshot

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

private[future] class LogPropagatorImpl(leaderId: Node, log: Log[Future], clients: RpcClientProvider[Future])(implicit
  EC: ExecutionContext,
  logger: Logger[Future]
) extends LogPropagator[Future] {

  private val installingRef = new AtomicReference[Set[Node]](Set.empty)

  override def propagateLogs(peerId: Node, term: Long, nextIndex: Long): Future[AppendEntriesResponse] =
    for {
      _        <- logger.trace(s"Replicating logs to ${peerId}. Term: ${term}, nextIndex: ${nextIndex}")
      _        <- snapshotIsNotInstalling(peerId)
      snapshot <- log.latestSnapshot
      response <-
        if (snapshot.exists(_.lastIndex >= nextIndex))
          sendSnapshot(peerId, snapshot.get)
        else
          log
            .getAppendEntries(leaderId, term, nextIndex)
            .flatMap(request => clients.send(peerId, request))

    } yield response

  private def sendSnapshot(peerId: Node, snapshot: Snapshot): Future[AppendEntriesResponse] = {
    val response = for {
      _ <- logger.trace(s"Installing an Snapshot for peer ${peerId}, snapshot: ${snapshot}")
      _ = installingRef.updateAndGet(_ + peerId)
      logEntry <- log.get(snapshot.lastIndex)
      response <- clients.send(peerId, snapshot, logEntry)
      _        <- logger.trace(s"Response after installing snapshot ${response}")
      _ = installingRef.updateAndGet(_ - peerId)
    } yield response

    response.recoverWith { error =>
      installingRef.updateAndGet(_ - peerId)
      logger.trace("Error during snapshot installation ${error}")

      Future.failed(error)
    }

  }

  private def snapshotIsNotInstalling(peerId: Node): Future[Unit] =
    if (installingRef.get().contains(peerId)) {
      Future.failed(new RuntimeException("Client is installing an snapshot"))
    } else {
      Future.successful(())
    }
}

object LogPropagatorImpl {
  def build(nodeId: Node, clients: RpcClientProvider[Future], log: Log[Future])(implicit
    EC: ExecutionContext,
    L: Logger[Future]
  ): LogPropagator[Future] =
    new LogPropagatorImpl(nodeId, log, clients)
}
