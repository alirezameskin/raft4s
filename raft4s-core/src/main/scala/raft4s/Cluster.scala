package raft4s

import cats.implicits._
import cats.Monad
import raft4s.internal.Raft
import raft4s.rpc.RpcServer

class Cluster[F[_]: Monad](rpc: RpcServer[F], raft: Raft[F]) {

  /**
   * It starts the cluster, and returns the `Leader` Node in response.
   */
  def start: F[Node] =
    for {
      _      <- raft.initialize
      _      <- rpc.start
      leader <- raft.start
    } yield leader

  /**
   * In stops the cluster, by stopping the underlying `Raft` implementation and `Rpc` server.
   */
  def stop: F[Unit] =
    for {
      _ <- rpc.stop
      _ <- raft.stop
    } yield ()

  /**
   * It adds a new Member to the cluster.
   * This method creates a new ClusterConfiguration with the new `Node` and propagates the new configuration among cluster members.
   * If the current `Node` is not the leader itself, it forwards the change to the current `Leader` of the `Cluster`.
   */
  def join(node: Node): F[Node] =
    for {
      _      <- raft.initialize
      _      <- rpc.start
      leader <- raft.join(node)
    } yield leader

  /**
   * It removed the current `Node` from the `Cluster`.
   * This methods creates a new ClusterConfiguration without its own Node and propagates the new configuration among cluster members.
   * If the current `Node` is not the leader itself, it forwards the change to the current `Leader` of the `Cluster`.
   */
  def leave: F[Unit] =
    raft.leave

  /**
   * It returns the current `Leader` of the Cluster.
   * If there is no selected `Leader` yet, it blocks until it is selected.
   */
  def leader: F[Node] =
    raft.listen

  /**
   * It executes a `Command`.
   * If it is a `WriteCommand`, this methods forwards it to the current `Leader`. since write commands should be applied on Leader node.
   * Depending on the Configuration, `ReadCommand`s can be applied on `Follower` nodes.
   */
  def execute[T](command: Command[T]): F[T] =
    raft.onCommand(command)
}
