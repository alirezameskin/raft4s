package raft4s.future

import cats.implicits._
import raft4s._
import raft4s.internal.Logger
import raft4s.rpc.{RpcClientBuilder, RpcServerBuilder}

import scala.concurrent.{ExecutionContext, Future}

object RaftCluster {

  def build(
    config: Configuration,
    storage: Storage[Future],
    stateMachine: StateMachine[Future],
    compactionPolicy: LogCompactionPolicy[Future]
  )(implicit
    EC: ExecutionContext,
    SB: RpcServerBuilder[Future],
    CB: RpcClientBuilder[Future],
    L: Logger[Future]
  ): Future[Cluster[Future]] =
    for {
      raft   <- RaftImpl.build(config, storage, stateMachine, compactionPolicy)
      server <- RpcServerBuilder[Future].build(config.local, raft)
    } yield new Cluster[Future](server, raft)
}
