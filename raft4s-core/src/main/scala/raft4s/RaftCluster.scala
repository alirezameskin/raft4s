package raft4s

import cats.effect.{Concurrent, Resource, Timer}
import cats.implicits._
import cats.{Monad, Parallel}
import io.odin.Logger
import raft4s.protocol.Command
import raft4s.rpc.{RpcClientBuilder, RpcServer, RpcServerBuilder}
import raft4s.storage.Storage

class RaftCluster[F[_]: Monad](rpc: RpcServer[F], raft: Raft[F]) {

  def start: F[String] =
    for {
      _      <- raft.initialize()
      _      <- rpc.start()
      leader <- raft.start()
    } yield leader

  def leader: F[String] =
    raft.listen()

  def execute[T](command: Command[T]): F[T] =
    raft.onCommand(command)
}

object RaftCluster {

  def resource[F[_]: Monad: Parallel: Concurrent: RpcServerBuilder: RpcClientBuilder: Timer: Logger](
    config: Configuration,
    storage: Storage[F],
    stateMachine: StateMachine[F]
  ): Resource[F, RaftCluster[F]] =
    for {
      raft   <- Raft.resource(config, storage, stateMachine)
      server <- RpcServerBuilder[F].resource(config.local, raft)
    } yield new RaftCluster[F](server, raft)
}
