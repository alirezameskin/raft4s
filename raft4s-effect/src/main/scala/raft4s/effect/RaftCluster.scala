package raft4s.effect

import cats.effect.{Async, Resource}
import cats.{Monad, Parallel}
import raft4s._
import raft4s.internal.Logger
import raft4s.rpc.{RpcClientBuilder, RpcServerBuilder}

object RaftCluster {

  def resource[F[_]: Async: Parallel: RpcServerBuilder: RpcClientBuilder: Logger](
    config: Configuration,
    storage: Storage[F],
    stateMachine: StateMachine[F]
  ): Resource[F, Cluster[F]] =
    resource(
      config,
      storage,
      stateMachine,
      if (config.logCompactionThreshold <= 0)
        LogCompactionPolicy.noCompaction
      else
        LogCompactionPolicy.fixedSize(config.logCompactionThreshold)
    )

  def resource[F[_]: Async: Parallel: RpcServerBuilder: RpcClientBuilder: Logger](
    config: Configuration,
    storage: Storage[F],
    stateMachine: StateMachine[F],
    compactionPolicy: LogCompactionPolicy[F]
  ): Resource[F, Cluster[F]] =
    for {
      raft   <- Resource.eval(RaftImpl.build(config, storage, stateMachine, compactionPolicy))
      server <- Resource.eval(RpcServerBuilder[F].build(config.local, raft))
      acquire = Monad[F].pure(new Cluster[F](server, raft))
      cluster <- Resource.make(acquire)(_.stop)
    } yield cluster
}
