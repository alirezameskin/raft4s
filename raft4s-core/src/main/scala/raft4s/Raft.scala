package raft4s

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.implicits._
import cats.{Monad, MonadError, Parallel}
import io.odin.Logger
import raft4s.internal.{ErrorLogging, LeaderAnnouncer, LogReplicator, RpcClientProvider}
import raft4s.log.Log
import raft4s.node._
import raft4s.protocol._
import raft4s.rpc._
import raft4s.storage.{StateStorage, Storage}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class Raft[F[_]: Monad: Concurrent: Timer: Parallel](
  config: Configuration,
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

  def start(): F[String] =
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

  def listen(): F[String] =
    errorLogging("Waiting for the Leader to be elected") {
      leaderAnnouncer.listen()
    }

  def onReceive(msg: VoteRequest): F[VoteResponse] =
    errorLogging("Receiving VoteRequest") {
      for {
        _        <- logger.trace(s"A Vote request received from ${msg.nodeId}, Term: ${msg.logTerm}, ${msg}")
        logState <- log.state
        result   <- state.modify(_.onReceive(logState, msg))

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
        actions  <- state.modify(_.onReceive(logState, msg))
        _        <- runActions(actions)
      } yield ()
    }

  def onReceive(msg: AppendEntries): F[AppendEntriesResponse] =
    errorLogging(s"Receiving an AppendEntries ${msg}") {
      for {
        _        <- logger.trace(s"A AppendEntries request received from ${msg.leaderId}, contains ${msg.entries.size} entries, ${msg}")
        logState <- log.state
        current  <- state.get
        time     <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
        _        <- lastHeartbeat.set(time)

        (nextState, (response, actions)) = current.onReceive(logState, msg)

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
        actions  <- state.modify(_.onReceive(logState, msg))
        _        <- runActions(actions)
      } yield ()
    }

  def onReceive(msg: InstallSnapshot): F[AppendEntriesResponse] =
    errorLogging("Receiving InstallSnapshot") {
      for {
        _        <- log.installSnapshot(msg.snapshot, msg.lastEntry)
        logState <- log.state
        response <- state.modify(_.onSnapshotInstalled(logState))
      } yield response
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
            actions  <- onWriteCommand(state_, command, deferred)
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

  private def onWriteCommand[T](node: NodeState, command: WriteCommand[T], deferred: Deferred[F, T]): F[List[Action]] =
    node match {
      case LeaderNode(_, _, term, _, _) =>
        if (config.members.isEmpty)
          for {
            _ <- logger.trace("Appending the command to the log")
            _ <- log.append(term, command, deferred)
            _ <- log.commitLogs(Map.empty, 0)
          } yield List.empty
        else
          for {
            _ <- logger.trace("Appending the command to the log")
            _ <- log.append(term, command, deferred)
          } yield node.onReplicateLog()

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
        for {
          _    <- logger.trace("Storing the new state in the storage")
          node <- state.get
          _    <- storage.stateStorage.persistState(node.toPersistedState)
        } yield ()

      case CommitLogs(ackedLength, minAckes) =>
        log.commitLogs(ackedLength, minAckes)

      case AnnounceLeader(leaderId, true) =>
        leaderAnnouncer.reset() *> leaderAnnouncer.announce(leaderId)

      case AnnounceLeader(leaderId, false) =>
        leaderAnnouncer.announce(leaderId)

      case ResetLeaderAnnouncer =>
        leaderAnnouncer.reset()

    }

  private def runElection(): F[Unit] =
    for {
      _        <- delayElection()
      logState <- log.state
      actions  <- state.modify(_.onTimer(logState))
      _        <- runActions(actions)
    } yield ()

  private def scheduleReplication(): F[Unit] = {
    val scheduled = for {
      _    <- Timer[F].sleep(FiniteDuration(config.heartbeatIntervalMillis, TimeUnit.MILLISECONDS))
      node <- state.get
      actions = if (node.isInstanceOf[LeaderNode]) node.onReplicateLog() else List.empty
      _ <- runActions(actions)
    } yield ()

    background {
      Monad[F].foreverM(scheduled)
    }
  }

  private def scheduleElection(): F[Unit] = {
    val scheduled = for {
      _    <- Timer[F].sleep(FiniteDuration(config.heartbeatTimeoutMillis, TimeUnit.MILLISECONDS))
      now  <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
      lh   <- lastHeartbeat.get
      node <- state.get
      _    <- if (!node.isInstanceOf[LeaderNode] && now - lh > config.heartbeatTimeoutMillis) runElection() else Monad[F].unit
    } yield ()

    background {
      Monad[F].foreverM(scheduled)
    }
  }

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

  def build[F[_]: Monad: Concurrent: Parallel: Timer: RpcClientBuilder: Logger](
    config: Configuration,
    storage: Storage[F],
    stateMachine: StateMachine[F]
  ): F[Raft[F]] =
    resource(config, storage, stateMachine).use(Monad[F].pure)

  def resource[F[_]: Monad: Concurrent: Parallel: Timer: RpcClientBuilder: Logger](
    config: Configuration,
    storage: Storage[F],
    stateMachine: StateMachine[F]
  ): Resource[F, Raft[F]] =
    for {
      clientProvider <- RpcClientProvider.resource[F](config.members)
      log            <- Resource.liftF(Log.build[F](storage.logStorage, storage.snapshotStorage, stateMachine))
      replicator     <- Resource.liftF(LogReplicator.build[F](config.nodeId, clientProvider, log))
      announcer      <- Resource.liftF(LeaderAnnouncer.build[F])
      heartbeat      <- Resource.liftF(Ref.of[F, Long](0L))
      state          <- Resource.liftF(latestState(config, storage.stateStorage))
      ref            <- Resource.liftF(Ref.of[F, NodeState](state))
    } yield new Raft[F](config, clientProvider, announcer, replicator, log, storage, ref, heartbeat)

  private def latestState[F[_]: Monad](config: Configuration, storage: StateStorage[F]): F[FollowerNode] =
    for {
      persisted <- storage.retrieveState()
    } yield FollowerNode(config.nodeId, config.nodes, persisted.map(_.term).getOrElse(0L), persisted.flatMap(_.votedFor))
}
