package raft4s.future

import cats.implicits._
import raft4s.internal.Logger
import raft4s.log.LogCompactionPolicy
import raft4s.rpc.RpcServerBuilder
import raft4s.{Cluster, Configuration, StateMachine, Storage}

import scala.concurrent.{ExecutionContext, Future}

object RaftCluster {

  def build(
    config: Configuration,
    storage: Storage[Future],
    stateMachine: StateMachine[Future],
    compactionPolicy: LogCompactionPolicy[Future]
  )(implicit EC: ExecutionContext, SB: RpcServerBuilder[Future], L : Logger[Future]): Future[Cluster[Future]] =
    for {
      raft   <- Raft.build(config, storage, stateMachine, compactionPolicy)
      server <- RpcServerBuilder[Future].build(config.local, raft)
    } yield new Cluster[Future](server, raft)
}
