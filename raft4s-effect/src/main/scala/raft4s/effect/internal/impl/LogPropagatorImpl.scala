package raft4s.effect.internal.impl

import cats.effect.{Concurrent, Ref}
import cats.implicits._
import raft4s.Node
import raft4s.internal.{LogPropagator, Logger}
import raft4s.protocol.AppendEntriesResponse
import raft4s.storage.Snapshot

import scala.collection.Set

private[effect] class LogPropagatorImpl[F[_]: Concurrent: Logger](
  leaderId: Node,
  log: LogImpl[F],
  clients: RpcClientProviderImpl[F],
  installingRef: Ref[F, Set[Node]]
) extends LogPropagator[F] {

  def propagateLogs(peerId: Node, term: Long, nextIndex: Long): F[AppendEntriesResponse] =
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

object LogPropagatorImpl {
  def build[F[_]: Concurrent: Logger](
    leaderId: Node,
    clients: RpcClientProviderImpl[F],
    log: LogImpl[F]
  ): F[LogPropagatorImpl[F]] =
    for {
      installing <- Ref.of[F, Set[Node]](Set.empty)
    } yield new LogPropagatorImpl[F](leaderId, log, clients, installing)
}
