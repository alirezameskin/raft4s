package raft4s.internal

import cats.effect.{Resource, Sync}
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.{Monad, MonadError}
import io.odin.Logger
import raft4s.Address
import raft4s.protocol._
import raft4s.rpc.{RpcClient, RpcClientBuilder}
import raft4s.storage.Snapshot

private[raft4s] class RpcClientProvider[F[_]: Monad: RpcClientBuilder: Logger](
  val clients: Ref[F, Map[String, RpcClient[F]]],
  val members: Seq[Address]
)(implicit ME: MonadError[F, Throwable]) {

  def send(serverId: String, voteRequest: VoteRequest): F[VoteResponse] =
    for {
      client  <- getClient(serverId)
      attempt <- client.send(voteRequest).attempt
      result  <- logErrors(serverId, attempt)
    } yield result

  def send(serverId: String, appendEntries: AppendEntries): F[AppendEntriesResponse] =
    for {
      client  <- getClient(serverId)
      attempt <- client.send(appendEntries).attempt
      result  <- logErrors(serverId, attempt)
    } yield result

  def send(serverId: String, snapshot: Snapshot, lastEntry: LogEntry): F[AppendEntriesResponse] =
    for {
      client   <- getClient(serverId)
      attempt  <- client.send(snapshot, lastEntry).attempt
      response <- logErrors(serverId, attempt)
    } yield response

  def send[T](serverId: String, command: Command[T]): F[T] =
    for {
      client  <- getClient(serverId)
      attempt <- client.send(command).attempt
      result  <- logErrors(serverId, attempt)
    } yield result

  private def logErrors[T](peerId: String, result: Either[Throwable, T]): F[T] =
    result match {
      case Left(error) =>
        Logger[F].warn(s"An error during communication with ${peerId}. Error: ${error}") *> ME.raiseError(error)
      case Right(value) =>
        Monad[F].pure(value)
    }

  private def getClient(serverId: String): F[RpcClient[F]] =
    for {
      maps <- clients.get
      possibleClient = maps.get(serverId)
      client <-
        if (possibleClient.isDefined)
          Monad[F].pure(possibleClient.get)
        else {
          val serverConfig = members.find(_.toString == serverId).get //TODO
          val c            = implicitly[RpcClientBuilder[F]].build(serverConfig)
          clients.updateAndGet(_ + (serverConfig.id -> c)) *> Monad[F].pure(c)

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

  def resource[F[_]: Monad: Sync: RpcClientBuilder: Logger](members: Seq[Address]): Resource[F, RpcClientProvider[F]] = {
    val acquire = for {
      clients <- Ref.of[F, Map[String, RpcClient[F]]](Map.empty)
    } yield new RpcClientProvider[F](clients, members)

    Resource.make(acquire)(pr => pr.closeConnections())
  }
}
