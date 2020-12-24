package raft4s.future.internal

import raft4s.internal.Logger
import raft4s.protocol.AppendEntriesResponse
import raft4s.storage.Snapshot
import raft4s.{internal, Node}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

private[future] class LogReplicator(leaderId: Node, log: internal.Log[Future], clients: internal.RpcClientProvider[Future])(
  implicit
  EC: ExecutionContext,
  logger: Logger[Future]
) extends raft4s.internal.LogReplicator[Future] {
  private val installingRef = new AtomicReference[Set[Node]](Set.empty)

  override def replicatedLogs(peerId: Node, term: Long, nextIndex: Long): Future[AppendEntriesResponse] =
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
            .map { req =>
              println(req)
              req
            }
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

object LogReplicator {
  def build(nodeId: Node, clients: raft4s.internal.RpcClientProvider[Future], log: raft4s.internal.Log[Future])(implicit
    EC: ExecutionContext,
    L: Logger[Future]
  ): raft4s.internal.LogReplicator[Future] =
    new LogReplicator(nodeId, log, clients)
}
