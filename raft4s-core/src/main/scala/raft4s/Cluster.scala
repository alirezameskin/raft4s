package raft4s

import cats.implicits._
import cats.Monad
import raft4s.internal.Raft
import raft4s.rpc.RpcServer

class Cluster[F[_]: Monad](rpc: RpcServer[F], raft: Raft[F]) {

  def start: F[Node] =
    for {
      _      <- raft.initialize
      _      <- rpc.start
      leader <- raft.start
    } yield leader

  def stop: F[Unit] =
    for {
      _ <- rpc.stop
      _ <- raft.stop
    } yield ()

  def join(node: Node): F[Node] =
    for {
      _      <- raft.initialize
      _      <- rpc.start
      leader <- raft.join(node)
    } yield leader

  def leave: F[Unit] =
    raft.leave

  def leader: F[Node] =
    raft.listen

  def execute[T](command: Command[T]): F[T] =
    raft.onCommand(command)
}
