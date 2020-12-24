package raft4s.future

import cats.MonadError
import raft4s.internal.{LeaderAnnouncer, LogReplicator, MembershipManager, RpcClientProvider, _}
import raft4s.node.{FollowerNode, LeaderNode, NodeState}
import raft4s.rpc.RpcClientBuilder
import raft4s._

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.{FiniteDuration, NANOSECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}

class Raft(
  val config: Configuration,
  val membershipManager: MembershipManager[Future],
  val clientProvider: RpcClientProvider[Future],
  val leaderAnnouncer: LeaderAnnouncer[Future],
  val logReplicator: LogReplicator[Future],
  val log: Log[Future],
  val storage: Storage[Future],
  state: NodeState
)(implicit val ME: MonadError[Future, Throwable], val logger: Logger[Future], EC: ExecutionContext)
    extends raft4s.internal.Raft[Future] {
  override val nodeId: Node = config.local

  private val lock = new ReentrantReadWriteLock()

  private val isRunningRef = new AtomicBoolean(false)
  private val heartbeatRef = new AtomicLong(0L)
  private val nodeStateRef = new AtomicReference[NodeState](state)

  private val scheduler = Executors.newScheduledThreadPool(1)

  override def setRunning(isRunning: Boolean): Future[Unit] = Future(isRunningRef.set(isRunning))

  override def getRunning: Future[Boolean] = Future(isRunningRef.get())

  override def getCurrentState: Future[NodeState] = Future(nodeStateRef.get())

  override def setCurrentState(state: NodeState): Future[Unit] = Future(nodeStateRef.set(state))

  override def withPermit[A](code: => Future[A]): Future[A] = {
    val currentLock = lock.writeLock()

    ME.attempt(code).flatMap {
      case Right(result) =>
        currentLock.unlock()
        Future.apply(result)

      case Left(error) =>
        currentLock.unlock()
        Future.failed(error)
    }
  }

  override def background[A](fa: => Future[A]): Future[Unit] =
    ME.attempt(fa).flatMap(_ => ME.unit)

  override def updateLastHeartbeat: Future[Unit] =
    ME.pure {
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
    ME.pure {
      val promise = Promise[A]

      new Deferred[Future, A] {
        override def get: Future[A] =
          promise.future

        override def complete(a: A): Future[Unit] =
          Future(promise.success(a))
      }
    }

  private def random(min: Long, max: Long): Long =
    min + scala.util.Random.nextLong(max - min)
}

object Raft {

  def build(
    config: Configuration,
    storage: Storage[Future],
    stateMachine: StateMachine[Future],
    compactionPolicy: LogCompactionPolicy[Future]
  )(implicit EC: ExecutionContext, L: Logger[Future], CB: RpcClientBuilder[Future]): Future[Raft] =
    for {
      persistedState <- storage.stateStorage.retrieveState()
      nodeState      = persistedState.map(_.toNodeState(config.local)).getOrElse(FollowerNode(config.local, 0L))
      appliedIndex   = persistedState.map(_.appliedIndex).getOrElse(0L)
      clientProvider = raft4s.future.internal.RpcClientProvider.build
      membership     = raft4s.future.internal.MembershipManager.build(config.members.toSet + config.local)
      log = raft4s.future.internal.Log.build(
        storage.logStorage,
        storage.snapshotStorage,
        stateMachine,
        compactionPolicy,
        membership,
        appliedIndex
      )
      replicator = raft4s.future.internal.LogReplicator.build(config.local, clientProvider, log)
      announcer  = raft4s.future.internal.LeaderAnnouncer.build
    } yield new Raft(
      config,
      membership,
      clientProvider,
      announcer,
      replicator,
      log,
      storage,
      nodeState
    )
}
