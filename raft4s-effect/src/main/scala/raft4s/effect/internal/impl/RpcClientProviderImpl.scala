package raft4s.effect.internal.impl

import cats.effect.{Ref, Sync}
import cats.implicits._
import cats.{Monad, MonadError}
import raft4s.internal.{ErrorLogging, Logger, RpcClientProvider}
import raft4s.protocol._
import raft4s.rpc.{RpcClient, RpcClientBuilder}
import raft4s.storage.Snapshot
import raft4s.{Command, LogEntry, Node}

private[effect] class RpcClientProviderImpl[F[_]: Monad: RpcClientBuilder: Logger](
  val clientsRef: Ref[F, Map[Node, RpcClient[F]]],
  val members: Seq[Node]
)(implicit ME: MonadError[F, Throwable])
    extends ErrorLogging[F]
    with RpcClientProvider[F] {

  def send(serverId: Node, voteRequest: VoteRequest): F[VoteResponse] =
    for {
      client  <- getClient(serverId)
      attempt <- client.send(voteRequest).attempt
      result  <- logErrors(serverId, attempt)
    } yield result

  def send(serverId: Node, appendEntries: AppendEntries): F[AppendEntriesResponse] =
    errorLogging("Sending AppendEtries") {
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
      maps <- clientsRef.get
      possibleClient = maps.get(serverId)
      client <-
        if (possibleClient.isDefined)
          Monad[F].pure(possibleClient.get)
        else {
          val c = implicitly[RpcClientBuilder[F]].build(serverId)
          clientsRef.updateAndGet(_ + (serverId -> c)) *> Monad[F].pure(c)

        }
    } yield client

  def closeConnections(): F[Unit] =
    for {
      _     <- Logger[F].trace("Close all connections to other members")
      items <- clientsRef.get
      _     <- items.values.toList.traverse(_.close())
    } yield ()
}

object RpcClientProviderImpl {

  def build[F[_]: Monad: Sync: RpcClientBuilder: Logger](members: Seq[Node]): F[RpcClientProviderImpl[F]] =
    for {
      clients <- Ref.of[F, Map[Node, RpcClient[F]]](Map.empty)
    } yield new RpcClientProviderImpl[F](clients, members)
}
