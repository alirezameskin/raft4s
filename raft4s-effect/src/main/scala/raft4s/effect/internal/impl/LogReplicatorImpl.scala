package raft4s.effect.internal.impl

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import raft4s.Node
import raft4s.internal.{LogReplicator, Logger}
import raft4s.protocol.AppendEntriesResponse
import raft4s.storage.Snapshot

import scala.collection.Set

private[effect] class LogReplicatorImpl[F[_]: Concurrent: Logger](
  leaderId: Node,
  log: LogImpl[F],
  clients: RpcClientProviderImpl[F],
  installingRef: Ref[F, Set[Node]]
) extends LogReplicator[F] {

  def replicatedLogs(peerId: Node, term: Long, nextIndex: Long): F[AppendEntriesResponse] =
    for {
      _        <- Logger[F].trace(s"Replicating logs to ${peerId}. Term: ${term}, nextIndex: ${nextIndex}")
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

  private def sendSnapshot(peerId: Node, snapshot: Snapshot): F[AppendEntriesResponse] = {
    val response = for {
      _        <- Logger[F].trace(s"Installing an Snapshot for peer ${peerId}, snapshot: ${snapshot}")
      _        <- installingRef.update(_ + peerId)
      logEntry <- log.get(snapshot.lastIndex)
      response <- clients.send(peerId, snapshot, logEntry)
      _        <- Logger[F].trace(s"Response after installing snapshot ${response}")
      _        <- installingRef.update(_ - peerId)
    } yield response

    Concurrent[F].onError(response) { case error =>
      Logger[F].trace(s"Error during snapshot installation ${error}") *> installingRef.update(_ - peerId)
    }
  }

  private def snapshotIsNotInstalling(peerId: Node): F[Unit] =
    for {
      set <- installingRef.get
      _ <-
        if (set.contains(peerId)) Concurrent[F].raiseError(new RuntimeException("Client is installing an snapshot"))
        else Concurrent[F].unit
    } yield ()
}

object LogReplicatorImpl {
  def build[F[_]: Concurrent: Logger](
    leaderId: Node,
    clients: RpcClientProviderImpl[F],
    log: LogImpl[F]
  ): F[LogReplicatorImpl[F]] =
    for {
      installing <- Ref.of[F, Set[Node]](Set.empty)
    } yield new LogReplicatorImpl[F](leaderId, log, clients, installing)
}
