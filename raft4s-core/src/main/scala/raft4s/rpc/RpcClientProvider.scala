package raft4s.rpc

import cats.Monad
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import raft4s.Address

class RpcClientProvider[F[_]: Monad: RpcClientBuilder](
  val clients: Ref[F, Map[String, RpcClient[F]]],
  val members: Seq[Address]
) {
  def client(serverId: String): F[RpcClient[F]] =
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
}

object RpcClientProvider {
  def build[F[_]: Monad: Sync: RpcClientBuilder](members: Seq[Address]): F[RpcClientProvider[F]] =
    for {
      clients <- Ref.of[F, Map[String, RpcClient[F]]](Map.empty)
    } yield new RpcClientProvider[F](clients, members)
}
