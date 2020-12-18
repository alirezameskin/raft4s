package raft4s.internal

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import io.odin.Logger
import raft4s.Node
import raft4s.log.Log
import raft4s.protocol.AppendEntriesResponse
import raft4s.storage.Snapshot

import scala.collection.Set

private[raft4s] class LogReplicator[F[_]: Concurrent: Logger](
  leaderId: Node,
  log: Log[F],
  clients: RpcClientProvider[F],
  installing: Ref[F, Set[Node]]
) {

  def replicatedLogs(peerId: Node, term: Long, sentLength: Long): F[AppendEntriesResponse] =
    for {
      _        <- Logger[F].trace(s"Replicating logs to ${peerId}. Term: ${term}, sentLength : ${sentLength}")
      _        <- snapshotIsNotInstalling(peerId)
      snapshot <- log.latestSnapshot
      response <-
        if (snapshot.exists(_.lastIndex > sentLength))
          sendSnapshot(peerId, snapshot.get)
        else
          log
            .getAppendEntries(leaderId, term, sentLength)
            .map { entries =>
              println(s"Append entries ${entries}")
              entries
            }
            .flatMap(request => clients.send(peerId, request))

    } yield response

  private def sendSnapshot(peerId: Node, snapshot: Snapshot): F[AppendEntriesResponse] = {
    val response = for {
      _        <- Logger[F].trace(s"Installing an Snapshot for peer ${peerId}, snapshot: ${snapshot}")
      _        <- installing.update(_ + peerId)
      logEntry <- log.get(snapshot.lastIndex)
      response <- clients.send(peerId, snapshot, logEntry)
      _        <- Logger[F].trace(s"Response after installing snapshot ${response}")
      _        <- installing.update(_ - peerId)
    } yield response

    Concurrent[F].onError(response) { case error =>
      Logger[F].trace(s"Error during snapshot installation ${error}") *> installing.update(_ - peerId)
    }
  }

  private def snapshotIsNotInstalling(peerId: Node): F[Unit] =
    for {
      set <- installing.get
      _ <-
        if (set.contains(peerId)) Concurrent[F].raiseError(new RuntimeException("Client is installing an snapshot"))
        else Concurrent[F].unit
    } yield ()
}

object LogReplicator {
  def build[F[_]: Concurrent: Logger](leaderId: Node, clients: RpcClientProvider[F], log: Log[F]): F[LogReplicator[F]] =
    for {
      installing <- Ref.of[F, Set[Node]](Set.empty)
    } yield new LogReplicator[F](leaderId, log, clients, installing)
}
