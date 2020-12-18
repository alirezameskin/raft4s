package raft4s

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.implicits._
import cats.{Applicative, Monad, MonadError, Parallel}
import io.odin.Logger
import raft4s.internal._
import raft4s.log.{Log, LogCompactionPolicy}
import raft4s.node._
import raft4s.protocol._
import raft4s.rpc._
import raft4s.storage.Storage

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class Raft[F[_]: Monad: Concurrent: Timer: Parallel](
  config: Configuration,
  membershipManager: MembershipManager[F],
  clientProvider: RpcClientProvider[F],
  leaderAnnouncer: LeaderAnnouncer[F],
  logReplicator: LogReplicator[F],
  log: Log[F],
  storage: Storage[F],
  state: Ref[F, NodeState],
  lastHeartbeat: Ref[F, Long]
)(implicit ME: MonadError[F, Throwable], logger: Logger[F])
    extends ErrorLogging[F] {

  def initialize(): F[Unit] =
    log.initialize()

  def start(): F[Node] =
    errorLogging("Starting Cluster") {
      for {
        _      <- logger.info("Cluster is starting")
        _      <- delayElection()
        node   <- state.get
        _      <- if (node.leader.isDefined) Monad[F].unit else runElection()
        _      <- scheduleElection()
        _      <- scheduleReplication()
        _      <- logger.trace("Waiting for the leader to be elected.")
        leader <- leaderAnnouncer.listen()
        _      <- logger.info(s"A Leader is elected. Leader: '${leader}'")
      } yield leader
    }

  def join(node: Node): F[Node] =
    errorLogging("Joining to a cluster") {
      for {
        _      <- logger.info("Cluster is joining")
        st     <- state.get
        _      <- logger.trace(s"State ${st}")
        res    <- clientProvider.join(node, config.local)
        _      <- logger.trace(s"CLuster is joined to ${node} ${res}")
        node   <- state.get
        _      <- if (node.leader.isDefined) Monad[F].unit else runElection()
        _      <- scheduleElection()
        _      <- scheduleReplication()
        _      <- logger.trace("Waiting for the leader to be elected.")
        leader <- leaderAnnouncer.listen()
        _      <- logger.info(s"A Leader is elected. Leader: '${leader}'")
      } yield leader
    }

  def leave(): F[Unit] =
    errorLogging("Leaving a cluster") {
      for {
        _ <- logger.info(s"Node ${config.local} is leaving the cluster")
        _ <- removeMember(config.local)
        _ <- logger.info(s"Node ${config.local} left the cluster.")
      } yield ()
    }

  def listen(): F[Node] =
    errorLogging("Waiting for the Leader to be elected") {
      leaderAnnouncer.listen()
    }

  def onReceive(msg: VoteRequest): F[VoteResponse] =
    errorLogging("Receiving VoteRequest") {
      for {
        _        <- logger.trace(s"A Vote request received from ${msg.nodeId}, Term: ${msg.logTerm}, ${msg}")
        logState <- log.state
        ss       <- state.get
        _        <- logger.trace(s"Current state ${ss}")
        config   <- membershipManager.getClusterConfiguration
        result   <- state.modify(_.onReceive(logState, config, msg))

        (response, actions) = result

        _ <- runActions(actions)
        _ <- logger.trace(s"Vote response to the request ${response}")
      } yield response
    }

  def onReceive(msg: VoteResponse): F[Unit] =
    errorLogging("Receiving VoteResponse") {
      for {
        _        <- logger.trace(s"A Vote response received from ${msg.nodeId}, Granted: ${msg.granted}, ${msg}")
        logState <- log.state
        config   <- membershipManager.getClusterConfiguration
        actions  <- state.modify(_.onReceive(logState, config, msg))
        _        <- runActions(actions)
      } yield ()
    }

  def onReceive(msg: AppendEntries): F[AppendEntriesResponse] =
    errorLogging(s"Receiving an AppendEntries ${msg}") {
      for {
        _        <- logger.trace(s"A AppendEntries request received from ${msg.leaderId}, contains ${msg.entries.size} entries, ${msg}")
        logState <- log.state
        config   <- membershipManager.getClusterConfiguration
        current  <- state.get
        _        <- logger.trace(s"Current state ${current}")
        time     <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
        _        <- lastHeartbeat.set(time)

        (nextState, (response, actions)) = current.onReceive(logState, config, msg)

        _ <- runActions(actions)
        _ <-
          if (response.success)
            log.appendEntries(msg.entries, msg.logLength, msg.leaderAppliedIndex) *> state.set(nextState)
          else
            Monad[F].unit
      } yield response
    }

  def onReceive(msg: AppendEntriesResponse): F[Unit] =
    errorLogging("Receiving AppendEntrriesResponse") {
      for {
        _        <- logger.trace(s"A AppendEntriesResponse received from ${msg.nodeId}. ${msg}")
        logState <- log.state
        config   <- membershipManager.getClusterConfiguration
        actions  <- state.modify(_.onReceive(logState, config, msg))
        _        <- runActions(actions)
      } yield ()
    }

  def onReceive(msg: InstallSnapshot): F[AppendEntriesResponse] =
    errorLogging("Receiving InstallSnapshot") {
      for {
        _        <- log.installSnapshot(msg.snapshot, msg.lastEntry)
        logState <- log.state
        config   <- membershipManager.getClusterConfiguration
        response <- state.modify(_.onSnapshotInstalled(logState, config))
      } yield response
    }

  def addMember(member: Node): F[Unit] =
    for {
      config <- membershipManager.getClusterConfiguration
      _      <- addMember(config, member)
    } yield ()

  private def addMember(config: ClusterConfiguration, member: Node): F[Unit] = {
    if (config.members.contains(member)) {
      Applicative[F].unit
    }

    val oldMembers = config.members
    val newMembers = oldMembers + member
    val newConfig  = JointClusterConfiguration(oldMembers, newMembers)

    for {
      _ <- membershipManager.setClusterConfiguration(newConfig)
      _ <- Logger[F].trace(s"Commiting a joint configuration ${newConfig}")
      _ <- onCommand[Unit](JointConfigurationCommand(oldMembers, newMembers))
      _ <- Logger[F].trace("Joint configuration is commited")
      _ <- onCommand[Unit](NewConfigurationCommand(newMembers))
      _ <- Logger[F].trace("New configuration is commited")
    } yield ()
  }

  def removeMember(member: Node): F[Unit] =
    for {
      config <- membershipManager.getClusterConfiguration
      _      <- removeMember(config, member)
    } yield ()

  private def removeMember(config: ClusterConfiguration, member: Node): F[Unit] = {
    if (!config.members.contains(member)) {
      Applicative[F].unit
    }

    val oldMembers = config.members.toSet
    val newMembers = oldMembers - member
    val newConfig  = JointClusterConfiguration(oldMembers, newMembers)

    for {
      _ <- membershipManager.setClusterConfiguration(newConfig)
      _ <- Logger[F].trace(s"Commiting a joint configuration ${newConfig}")
      _ <- onCommand[Unit](JointConfigurationCommand(oldMembers, newMembers))
      _ <- Logger[F].trace("Joint configuration is commited")
      _ <- onCommand[Unit](NewConfigurationCommand(newMembers))
      _ <- Logger[F].trace("New configuration is commited")
    } yield ()
  }

  def onCommand[T](command: Command[T]): F[T] =
    errorLogging("Receiving Command") {
      command match {
        case command: ReadCommand[_] =>
          for {
            _      <- logger.trace(s"A read comment received ${command}")
            state_ <- state.get
            result <- onReadCommand(state_, command)
          } yield result

        case command: WriteCommand[_] =>
          for {
            _        <- logger.trace(s"A write comment received ${command}")
            deferred <- Deferred[F, T]
            state_   <- state.get
            config   <- membershipManager.getClusterConfiguration
            actions  <- onWriteCommand(state_, config, command, deferred)
            _        <- runActions(actions)
            result   <- deferred.get
          } yield result
      }
    }

  private def onReadCommand[T](node: NodeState, command: ReadCommand[T]): F[T] =
    node match {
      case _: LeaderNode =>
        for {
          _   <- logger.trace("Current node is the leader, it is running the read command")
          res <- log.applyReadCommand(command)
        } yield res

      case _: FollowerNode if config.followerAcceptRead =>
        for {
          _   <- logger.trace("Current node is a follower, it is running the read command")
          res <- log.applyReadCommand(command)
        } yield res

      case _ =>
        for {
          _        <- logger.trace("Read command has to be ran on the leader node")
          leader   <- leaderAnnouncer.listen()
          _        <- logger.trace(s"The current leader is ${leader}")
          response <- clientProvider.send(leader, command)
          _        <- logger.trace("Response for the read command received from the leader")
        } yield response
    }

  private def onWriteCommand[T](
    node: NodeState,
    cluster: ClusterConfiguration,
    command: WriteCommand[T],
    deferred: Deferred[F, T]
  ): F[List[Action]] =
    node match {
      case LeaderNode(_, term, _, _) =>
        if (cluster.members.size == 1)
          for {
            _         <- logger.trace(s"Appending the command to the log -  ${cluster.members}")
            entry     <- log.append(term, command, deferred)
            _         <- logger.trace(s"Entry appended ${entry}")
            committed <- log.commitLogs(Map(config.local -> (entry.index + 1)))
            _         <- if (committed) storeState() else Monad[F].unit
          } yield List.empty
        else
          for {
            _ <- logger.trace(s"Appending the command to the log ${cluster.members}")
            _ <- log.append(term, command, deferred)
          } yield node.onReplicateLog(cluster)

      case _ =>
        for {
          _        <- logger.trace("Write commands should be forwarded to the leader node.")
          leader   <- leaderAnnouncer.listen()
          _        <- logger.trace(s"The current leader is ${leader}.")
          response <- clientProvider.send(leader, command)
          _        <- logger.trace("Response for the write command received from the leader")
          _        <- deferred.complete(response)
        } yield List.empty
    }

  private def runActions(actions: List[Action]): F[Unit] =
    actions.traverse(action => runAction(action).attempt) *> Monad[F].unit

  private def runAction(action: Action): F[Unit] =
    action match {
      case RequestForVote(peerId, request) =>
        background {
          for {
            _        <- logger.trace(s"Sending a vote request to ${peerId}. Request: ${request}")
            response <- clientProvider.send(peerId, request)
            _        <- this.onReceive(response)
          } yield response
        }

      case ReplicateLog(peerId, term, sentLength) =>
        background {
          for {
            response <- logReplicator.replicatedLogs(peerId, term, sentLength)
            _        <- this.onReceive(response)
          } yield ()
        }

      case StoreState =>
        storeState()

      case CommitLogs(ackedLength) =>
        for {
          committed <- log.commitLogs(ackedLength)
          _         <- if (committed) storeState() else Monad[F].unit
        } yield ()

      case AnnounceLeader(leaderId, true) =>
        leaderAnnouncer.reset() *> leaderAnnouncer.announce(leaderId)

      case AnnounceLeader(leaderId, false) =>
        logger.trace("Anouncing a new leader without resetting ") *> leaderAnnouncer.announce(leaderId)

      case ResetLeaderAnnouncer =>
        leaderAnnouncer.reset()

    }

  private def storeState(): F[Unit] =
    for {
      _        <- logger.trace("Storing the new state in the storage")
      logState <- log.state
      node     <- state.get
      _        <- storage.stateStorage.persistState(node.toPersistedState.copy(appliedIndex = logState.appliedIndex))
    } yield ()

  private def runElection(): F[Unit] =
    for {
      _        <- delayElection()
      logState <- log.state
      cluster  <- membershipManager.getClusterConfiguration
      actions  <- state.modify(_.onTimer(logState, cluster))
      _        <- runActions(actions)
    } yield ()

  private def scheduleReplication(): F[Unit] = {
    val scheduled = for {
      _      <- Timer[F].sleep(FiniteDuration(config.heartbeatIntervalMillis, TimeUnit.MILLISECONDS))
      node   <- state.get
      config <- membershipManager.getClusterConfiguration
      actions = if (node.isInstanceOf[LeaderNode]) node.onReplicateLog(config) else List.empty
      _ <- runActions(actions)
    } yield ()

    background {
      Monad[F].foreverM(scheduled)
    }
  }

  private def scheduleElection(): F[Unit] =
    background {
      Monad[F].foreverM {
        for {
          _     <- Timer[F].sleep(FiniteDuration(config.heartbeatTimeoutMillis, TimeUnit.MILLISECONDS))
          alive <- isLeaderStillAlive
          _     <- if (alive) Monad[F].unit else runElection()
        } yield ()
      }
    }

  private def isLeaderStillAlive: F[Boolean] =
    for {
      node <- state.get
      lh   <- lastHeartbeat.get
      now  <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
    } yield node.isInstanceOf[LeaderNode] || (now - lh < config.heartbeatTimeoutMillis)

  private def background[A](fa: F[A]): F[Unit] =
    Concurrent[F].start(fa) *> Monad[F].unit

  private def delayElection(): F[Unit] =
    for {
      millis <- random(config.electionMinDelayMillis, config.electionMaxDelayMillis)
      delay  <- Sync[F].delay(FiniteDuration(millis, TimeUnit.MILLISECONDS))
      _      <- logger.trace(s"Delay to start the election ${delay}")
      _      <- Timer[F].sleep(delay)
    } yield ()

  private def random(min: Int, max: Int): F[Int] =
    Sync[F].delay(min + scala.util.Random.nextInt(max - min))
}

object Raft {

  def resource[F[_]: Monad: Concurrent: Parallel: Timer: RpcClientBuilder: Logger](
    config: Configuration,
    storage: Storage[F],
    stateMachine: StateMachine[F],
    compactionPolicy: LogCompactionPolicy[F]
  ): Resource[F, Raft[F]] =
    for {
      persistedState <- Resource.liftF(storage.stateStorage.retrieveState())

      appliedIndex = persistedState.map(_.appliedIndex).getOrElse(-1L)
      state        = FollowerNode(config.local, persistedState.map(_.term).getOrElse(0L), persistedState.flatMap(_.votedFor))

      clientProvider <- RpcClientProvider.resource[F](config.members)
      membership     <- Resource.liftF(MembershipManager.build[F](config.members.toSet + config.local))
      log <- Resource.liftF(
        Log.build[F](storage.logStorage, storage.snapshotStorage, stateMachine, compactionPolicy, membership, appliedIndex)
      )
      replicator <- Resource.liftF(LogReplicator.build[F](config.local, clientProvider, log))
      announcer  <- Resource.liftF(LeaderAnnouncer.build[F])
      heartbeat  <- Resource.liftF(Ref.of[F, Long](0L))
      ref        <- Resource.liftF(Ref.of[F, NodeState](state))
    } yield new Raft[F](config, membership, clientProvider, announcer, replicator, log, storage, ref, heartbeat)

}
