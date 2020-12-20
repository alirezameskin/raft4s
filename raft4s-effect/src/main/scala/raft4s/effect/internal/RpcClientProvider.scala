package raft4s.effect.internal

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.{Monad, MonadError}
import raft4s.Node
import raft4s.internal.{ErrorLogging, Logger}
import raft4s.protocol._
import raft4s.rpc.{RpcClient, RpcClientBuilder}
import raft4s.storage.Snapshot

private[effect] class RpcClientProvider[F[_]: Monad: RpcClientBuilder: Logger](
  val clients: Ref[F, Map[Node, RpcClient[F]]],
  val members: Seq[Node]
)(implicit ME: MonadError[F, Throwable])
    extends ErrorLogging[F]
    with raft4s.internal.RpcClientProvider[F] {

  def send(serverId: Node, voteRequest: VoteRequest): F[VoteResponse] =
    for {
      client  <- getClient(serverId)
      attempt <- client.send(voteRequest).attempt
      result  <- logErrors(serverId, attempt)
    } yield result

  def send(serverId: Node, appendEntries: AppendEntries): F[AppendEntriesResponse] =
    errorLogging("Sending AppendEtrries") {
      for {
        client  <- getClient(serverId)
        _       <- Logger[F].trace(s"Sending request ${appendEntries} to ${serverId} client: ${client}")
        attempt <- client.send(appendEntries).attempt
        _       <- Logger[F].trace(s"Attempt ${attempt}")
        result  <- logErrors(serverId, attempt)
      } yield result
    }

  def send(serverId: Node, snapshot: Snapshot, lastEntry: LogEntry): F[AppendEntriesResponse] =
    for {
      client   <- getClient(serverId)
      attempt  <- client.send(snapshot, lastEntry).attempt
      response <- logErrors(serverId, attempt)
    } yield response

  def send[T](serverId: Node, command: Command[T]): F[T] =
    for {
      client  <- getClient(serverId)
      attempt <- client.send(command).attempt
      result  <- logErrors(serverId, attempt)
    } yield result

  def join(serverId: Node, newNode: Node): F[Boolean] =
    for {
      client  <- getClient(serverId)
      attempt <- client.join(newNode).attempt
      result  <- logErrors(serverId, attempt)
    } yield result

  private def logErrors[T](peerId: Node, result: Either[Throwable, T]): F[T] =
    result match {
      case Left(error) =>
        Logger[F].warn(s"An error during communication with ${peerId}. Error: ${error}") *> ME.raiseError(error)
      case Right(value) =>
        Monad[F].pure(value)
    }

  private def getClient(serverId: Node): F[RpcClient[F]] =
    for {
      maps <- clients.get
      possibleClient = maps.get(serverId)
      client <-
        if (possibleClient.isDefined)
          Monad[F].pure(possibleClient.get)
        else {
          val c = implicitly[RpcClientBuilder[F]].build(serverId)
          clients.updateAndGet(_ + (serverId -> c)) *> Monad[F].pure(c)

        }
    } yield client

  def closeConnections(): F[Unit] =
    for {
      _     <- Logger[F].trace("Close all connections to other members")
      items <- clients.get
      _     <- items.values.toList.traverse(_.close())
    } yield ()
}

object RpcClientProvider {

  def build[F[_]: Monad: Sync: RpcClientBuilder: Logger](members: Seq[Node]): F[RpcClientProvider[F]] =
    for {
      clients <- Ref.of[F, Map[Node, RpcClient[F]]](Map.empty)
    } yield new RpcClientProvider[F](clients, members)
}