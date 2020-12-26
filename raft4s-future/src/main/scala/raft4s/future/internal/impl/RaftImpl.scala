package raft4s.future.internal.impl

import cats.MonadError
import raft4s._
import raft4s.internal._
import raft4s.node.{FollowerNode, LeaderNode, NodeState}
import raft4s.rpc.RpcClientBuilder

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.{FiniteDuration, NANOSECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}

private[future] class RaftImpl(
  val config: Configuration,
  val membershipManager: MembershipManager[Future],
  val clientProvider: RpcClientProvider[Future],
  val leaderAnnouncer: LeaderAnnouncer[Future],
  val logReplicator: LogReplicator[Future],
  val log: Log[Future],
  val storage: Storage[Future],
  state: NodeState
)(implicit val ME: MonadError[Future, Throwable], val logger: Logger[Future], EC: ExecutionContext)
    extends Raft[Future] {
  override val nodeId: Node = config.local

  private val isRunningRef = new AtomicBoolean(false)
  private val heartbeatRef = new AtomicLong(0L)
  private val nodeStateRef = new AtomicReference[NodeState](state)

  private val scheduler = Executors.newScheduledThreadPool(1)

  override def setRunning(isRunning: Boolean): Future[Unit] = Future(isRunningRef.set(isRunning))

  override def getRunning: Future[Boolean] =
    Future {
      isRunningRef.get()
    }

  override def getCurrentState: Future[NodeState] =
    Future {
      nodeStateRef.get()
    }

  override def setCurrentState(state: NodeState): Future[Unit] =
    Future {
      nodeStateRef.set(state)
    }

  override def background[A](fa: => Future[A]): Future[Unit] =
    ME.attempt(fa).flatMap(_ => ME.unit)

  override def updateLastHeartbeat: Future[Unit] =
    Future {
      heartbeatRef.set(TimeUnit.MILLISECONDS.convert(System.nanoTime(), NANOSECONDS))
    }

  override def electionTimeoutElapsed: Future[Boolean] =
    for {
      node <- getCurrentState
      lh  = heartbeatRef.get
      now = TimeUnit.MILLISECONDS.convert(System.nanoTime(), NANOSECONDS)
    } yield node.isInstanceOf[LeaderNode] || (now - lh < config.heartbeatTimeoutMillis)

  override def delayElection(): Future[Unit] = {
    val millis  = random(config.electionMinDelayMillis, config.electionMaxDelayMillis)
    val promise = Promise[Unit]()

    scheduler.schedule(
      () => promise.success(()),
      millis,
      TimeUnit.MILLISECONDS
    )

    promise.future
  }

  override def schedule(delay: FiniteDuration)(fa: => Future[Unit]): Future[Unit] =
    for {
      _ <- ME.pure(scheduler.scheduleAtFixedRate(() => fa, 0L, delay.toMillis, TimeUnit.MILLISECONDS))
    } yield ()

  override def emptyDeferred[A]: Future[Deferred[Future, A]] =
    Future {
      val promise = Promise[A]

      new Deferred[Future, A] {
        override def get: Future[A] =
          promise.future

        override def complete(a: A): Future[Unit] =
          Future(promise.success(a))
      }
    }

  override def stop: Future[Unit] =
    for {
      _ <- super.stop
      _ <- logger.trace("Stopping the scheduler")
      _ <- Future(scheduler.shutdown())
    } yield ()

  private def random(min: Long, max: Long): Long =
    min + scala.util.Random.nextLong(max - min)
}

object RaftImpl {

  def build(
    config: Configuration,
    storage: Storage[Future],
    stateMachine: StateMachine[Future],
    compactionPolicy: LogCompactionPolicy[Future]
  )(implicit EC: ExecutionContext, L: Logger[Future], CB: RpcClientBuilder[Future]): Future[RaftImpl] =
    for {
      persistedState <- storage.stateStorage.retrieveState()
      nodeState      = persistedState.map(_.toNodeState(config.local)).getOrElse(FollowerNode(config.local, 0L))
      appliedIndex   = persistedState.map(_.appliedIndex).getOrElse(0L)
      clientProvider = RpcClientProviderImpl.build
      membership     = MembershipManagerImpl.build(config.members.toSet + config.local)
      log            = LogImpl.build(storage.logStorage, storage.snapshotStorage, stateMachine, compactionPolicy, membership, appliedIndex)
      replicator     = LogReplicatorImpl.build(config.local, clientProvider, log)
      announcer      = LeaderAnnouncerImpl.build
    } yield new RaftImpl(config, membership, clientProvider, announcer, replicator, log, storage, nodeState)
}
