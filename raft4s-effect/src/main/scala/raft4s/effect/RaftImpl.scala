package raft4s.effect

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Sync, Timer}
import cats.implicits._
import cats.{Monad, MonadError, Parallel}
import raft4s._
import raft4s.effect.internal.impl._
import raft4s.internal.{Deferred, Logger}
import raft4s.node.{FollowerNode, LeaderNode, NodeState}
import raft4s.rpc.RpcClientBuilder

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

private[effect] class RaftImpl[F[_]: Monad: Concurrent: Timer](
  val config: Configuration,
  val membershipManager: MembershipManagerImpl[F],
  val clientProvider: RpcClientProviderImpl[F],
  val leaderAnnouncer: LeaderAnnouncerImpl[F],
  val logReplicator: LogPropagatorImpl[F],
  val log: LogImpl[F],
  val storage: Storage[F],
  stateRef: Ref[F, NodeState],
  lastHeartbeatRef: Ref[F, Long],
  isRunning: Ref[F, Boolean]
)(implicit val ME: MonadError[F, Throwable], val logger: Logger[F])
    extends Raft[F] {

  override val nodeId: Node = config.local

  override def setRunning(running: Boolean): F[Unit] =
    isRunning.set(running)

  override def getRunning: F[Boolean] =
    isRunning.get

  override def getCurrentState: F[NodeState] =
    stateRef.get

  override def setCurrentState(state: NodeState): F[Unit] =
    stateRef.set(state)

  override def background[A](fa: => F[A]): F[Unit] =
    Concurrent[F].start(fa) *> Monad[F].unit

  override def updateLastHeartbeat: F[Unit] =
    for {
      _    <- logger.trace(s"Update Last heartbeat time")
      time <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
      _    <- lastHeartbeatRef.set(time)
    } yield ()

  override def electionTimeoutElapsed: F[Boolean] =
    for {
      node <- getCurrentState
      lh   <- lastHeartbeatRef.get
      now  <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
    } yield node.isInstanceOf[LeaderNode] || (now - lh < config.heartbeatTimeoutMillis)

  override def delayElection(): F[Unit] =
    for {
      millis <- random(config.electionMinDelayMillis, config.electionMaxDelayMillis)
      delay  <- Sync[F].delay(FiniteDuration(millis, TimeUnit.MILLISECONDS))
      _      <- logger.trace(s"Delay to start the election ${delay}")
      _      <- Timer[F].sleep(delay)
    } yield ()

  override def schedule(delay: FiniteDuration)(fa: => F[Unit]): F[Unit] =
    Monad[F]
      .foreverM {
        for {
          _ <- Timer[F].sleep(delay)
          _ <- fa
        } yield ()
      }
      .whileM_(isRunning.get)

  override def emptyDeferred[A]: F[Deferred[F, A]] =
    for {
      underlying <- cats.effect.concurrent.Deferred[F, A]
    } yield new Deferred[F, A] {
      override def get: F[A] = underlying.get

      override def complete(a: A): F[Unit] = underlying.complete(a)
    }

  private def random(min: Int, max: Int): F[Int] =
    Sync[F].delay(min + scala.util.Random.nextInt(max - min))
}

object RaftImpl {

  def build[F[_]: Monad: Concurrent: Parallel: Timer: RpcClientBuilder: Logger](
    config: Configuration,
    storage: Storage[F],
    stateMachine: StateMachine[F],
    compactionPolicy: LogCompactionPolicy[F]
  ): F[RaftImpl[F]] =
    for {
      persistedState <- storage.stateStorage.retrieveState()
      nodeState    = persistedState.map(_.toNodeState(config.local)).getOrElse(FollowerNode(config.local, 0L))
      appliedIndex = persistedState.map(_.appliedIndex).getOrElse(0L)
      clientProvider <- RpcClientProviderImpl.build[F](config.members)
      membership     <- MembershipManagerImpl.build[F](config.members.toSet + config.local)
      log <- LogImpl
        .build[F](storage.logStorage, storage.snapshotStorage, stateMachine, compactionPolicy, membership, appliedIndex)
      replicator <- LogPropagatorImpl.build[F](config.local, clientProvider, log)
      announcer  <- LeaderAnnouncerImpl.build[F]
      heartbeat  <- Ref.of[F, Long](0L)
      ref        <- Ref.of[F, NodeState](nodeState)
      running    <- Ref.of[F, Boolean](false)
    } yield new RaftImpl[F](config, membership, clientProvider, announcer, replicator, log, storage, ref, heartbeat, running)

}
