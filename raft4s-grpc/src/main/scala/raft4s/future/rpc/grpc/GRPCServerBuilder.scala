package raft4s.future.rpc.grpc

import io.grpc.ServerBuilder
import raft4s.{Node, Raft}
import raft4s.future.rpc.grpc.internal.GRPCRaftService
import raft4s.grpc.protos
import raft4s.internal.Logger
import raft4s.rpc.grpc.serializer.Serializer
import raft4s.rpc.{RpcServer, RpcServerBuilder}

import java.util.concurrent.TimeUnit
import scala.concurrent.{blocking, ExecutionContext, Future}

class GRPCServerBuilder(implicit EC: ExecutionContext, L: Logger[Future], S: Serializer) extends RpcServerBuilder[Future] {

  override def build(node: Node, raft: Raft[Future]): Future[RpcServer[Future]] =
    Future {
      val service = protos.RaftGrpc.bindService(new GRPCRaftService(raft, S), EC);

      val builder: ServerBuilder[_] = ServerBuilder
        .forPort(node.port)
        .addService(service)

      val server = builder.build()

      new RpcServer[Future] {
        override def start: Future[Unit] =
          Future(server.start())

        override def stop: Future[Unit] =
          Future {
            server.shutdown()
            if (!blocking(server.awaitTermination(30, TimeUnit.SECONDS))) {
              server.shutdownNow()
              ()
            }
          }
      }
    }
}
