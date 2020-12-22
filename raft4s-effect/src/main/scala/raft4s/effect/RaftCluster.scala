package raft4s.effect

import cats.effect.{Concurrent, Resource, Timer}
import cats.{Monad, Parallel}
import raft4s._
import raft4s.effect.internal.Raft
import raft4s.internal.Logger
import raft4s.rpc.{RpcClientBuilder, RpcServerBuilder}

object RaftCluster {

  def resource[F[_]: Monad: Parallel: Concurrent: RpcServerBuilder: RpcClientBuilder: Timer: Logger](
    config: Configuration,
    storage: Storage[F],
    stateMachine: StateMachine[F]
  ): Resource[F, Cluster[F]] =
    resource(
      config,
      storage,
      stateMachine,
      FixedSizeLogCompaction(config.logCompactionThreshold)
    )

  def resource[F[_]: Monad: Parallel: Concurrent: RpcServerBuilder: RpcClientBuilder: Timer: Logger](
    config: Configuration,
    storage: Storage[F],
    stateMachine: StateMachine[F],
    compactionPolicy: LogCompactionPolicy[F]
  ): Resource[F, Cluster[F]] =
    for {
      raft   <- Resource.liftF(Raft.build(config, storage, stateMachine, compactionPolicy))
      server <- Resource.liftF(RpcServerBuilder[F].build(config.local, raft))
      acquire = Monad[F].pure(new Cluster[F](server, raft))
      cluster <- Resource.make(acquire)(_.stop)
    } yield cluster
}
