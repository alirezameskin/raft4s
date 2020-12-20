package raft4s

import cats.implicits._
import cats.{Applicative, Monad, MonadError}
import raft4s.internal._
import raft4s.node.{FollowerNode, LeaderNode, NodeState}
import raft4s.protocol._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

abstract private[raft4s] class Raft[F[_]: Monad] extends ErrorLogging[F] {
  implicit val logger: Logger[F]
  implicit val ME: MonadError[F, Throwable]

  val nodeId: Node
  val config: Configuration
  val log: Log[F]
  val storage: Storage[F]

  val leaderAnnouncer: LeaderAnnouncer[F]

  val clientProvider: RpcClientProvider[F]

  val membershipManager: MembershipManager[F]

  val logReplicator: LogReplicator[F]

  def setRunning(isRunning: Boolean): F[Unit]

  def getRunning: F[Boolean]

  def getCurrentState: F[NodeState]

  def setCurrentState(state: NodeState): F[Unit]

  def withPermit[A](t: => F[A]): F[A]

  def background[A](fa: => F[A]): F[Unit]

  def updateLastHeartbeat: F[Unit]

  def electionTimeoutElapsed: F[Boolean]

  def delayElection(): F[Unit]

  def schedule(delay: FiniteDuration)(fa: => F[Unit]): F[Unit]

  def emptyDeferred[A]: F[Deferred[F, A]]

  def initialize: F[Unit] =
    log.initialize()

  def start: F[Node] =
    errorLogging("Starting Cluster") {
      for {
        _      <- setRunning(true)
        _      <- logger.info("Cluster is starting")
        _      <- delayElection()
        node   <- getCurrentState
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
        _      <- setRunning(true)
        _      <- logger.info("Cluster is joining")
        res    <- clientProvider.join(node, nodeId)
        _      <- logger.trace(s"CLuster is joined to ${node} ${res}")
        node   <- getCurrentState
        _      <- if (node.leader.isDefined) Monad[F].unit else runElection()
        _      <- scheduleElection()
        _      <- scheduleReplication()
        _      <- logger.trace("Waiting for the leader to be elected.")
        leader <- leaderAnnouncer.listen()
        _      <- logger.info(s"A Leader is elected. Leader: '${leader}'")
      } yield leader
    }

  def stop: F[Unit] =
    errorLogging("Stopping a Cluster") {
      for {
        _ <- logger.info("Stopping the cluster")
        _ <- setRunning(false)
        _ <- clientProvider.closeConnections()
        _ <- logger.info("Cluster stopped")
      } yield ()
    }

  def leave: F[Unit] =
    errorLogging("Leaving a cluster") {
      for {
        _ <- logger.info(s"Node ${nodeId} is leaving the cluster")
        _ <- removeMember(nodeId)
        _ <- logger.info(s"Node ${nodeId} left the cluster.")
        _ <- setRunning(false)
      } yield ()
    }

  def listen: F[Node] =
    errorLogging("Waiting for the Leader to be elected") {
      leaderAnnouncer.listen()
    }

  def onReceive(msg: VoteRequest): F[VoteResponse] =
    errorLogging("Receiving VoteRequest") {
      withPermit {
        for {
          _            <- logger.trace(s"A Vote request received from ${msg.nodeId}, Term: ${msg.lastLogTerm}, ${msg}")
          logState     <- log.state
          config       <- membershipManager.getClusterConfiguration
          currentState <- getCurrentState
          result = currentState.onReceive(logState, config, msg)
          _ <- setCurrentState(result._1)

          (response, actions) = result._2

          _ <- runActions(actions)
          _ <- logger.trace(s"Vote response to the request ${response}")
          _ <- if (response.voteGranted) updateLastHeartbeat else Monad[F].unit
        } yield response
      }
    }

  def onReceive(msg: VoteResponse): F[Unit] =
    errorLogging("Receiving VoteResponse") {
      withPermit {
        for {
          _            <- logger.trace(s"A Vote response received from ${msg.nodeId}, Granted: ${msg.voteGranted}, ${msg}")
          logState     <- log.state
          config       <- membershipManager.getClusterConfiguration
          currentState <- getCurrentState
          result = currentState.onReceive(logState, config, msg)
          _ <- setCurrentState(result._1)
          actions = result._2
          _ <- runActions(actions)
        } yield ()
      }
    }

  def onReceive(msg: AppendEntries): F[AppendEntriesResponse] =
    errorLogging(s"Receiving an AppendEntries Term: ${msg.term} PreviousLogIndex:${msg.prevLogIndex}") {
      withPermit {
        for {
          _ <- logger.trace(
            s"A AppendEntries request received from ${msg.leaderId}, contains ${msg.entries.size} entries, ${msg}"
          )
          logState      <- log.state
          localPreEntry <- log.get(msg.prevLogIndex).map(Option(_))
          config        <- membershipManager.getClusterConfiguration
          current       <- getCurrentState
          _             <- logger.trace(s"Current state ${current}")
          _             <- logger.trace(s"Log state ${logState}")
          _             <- updateLastHeartbeat

          (nextState, (response, actions)) = current.onReceive(logState, config, msg, localPreEntry)

          _ <- setCurrentState(nextState)
          _ <- logger.trace(s"AppendEntriesReponse ${response}")
          _ <- logger.trace(s"Actions ${actions}")
          _ <- runActions(actions)
          appended <-
            if (response.success) {
              for {
                appended <- log.appendEntries(msg.entries, msg.prevLogIndex, msg.leaderCommit)
              } yield appended
            } else
              Monad[F].pure(false)
          _ <- if (appended) storeState() else Monad[F].unit
        } yield response
      }
    }

  def onReceive(msg: AppendEntriesResponse): F[Unit] =
    errorLogging("Receiving AppendEntrriesResponse") {
      withPermit {
        for {
          _            <- logger.trace(s"A AppendEntriesResponse received from ${msg.nodeId}. ${msg}")
          logState     <- log.state
          config       <- membershipManager.getClusterConfiguration
          currentState <- getCurrentState
          result = currentState.onReceive(logState, config, msg)
          _ <- setCurrentState(result._1)
          actions = result._2
          _ <- logger.trace(s"Actions ${actions}")
          _ <- runActions(actions)
        } yield ()
      }
    }

  def onReceive(msg: InstallSnapshot): F[AppendEntriesResponse] =
    errorLogging("Receiving InstallSnapshot") {
      withPermit {
        for {
          _            <- log.installSnapshot(msg.snapshot, msg.lastEntry)
          logState     <- log.state
          config       <- membershipManager.getClusterConfiguration
          currentState <- getCurrentState
          result = currentState.onSnapshotInstalled(logState, config)
          _ <- setCurrentState(result._1)
          response = result._2
        } yield response
      }
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
      _ <- logger.trace(s"Commiting a joint configuration ${newConfig}")
      _ <- onCommand[Unit](JointConfigurationCommand(oldMembers, newMembers))
      _ <- logger.trace("Joint configuration is commited")
      _ <- onCommand[Unit](NewConfigurationCommand(newMembers))
      _ <- logger.trace("New configuration is commited")
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
      _ <- logger.trace(s"Committing a joint configuration ${newConfig}")
      _ <- onCommand[Unit](JointConfigurationCommand(oldMembers, newMembers))
      _ <- logger.trace("Joint configuration is committed")
      _ <- onCommand[Unit](NewConfigurationCommand(newMembers))
      _ <- logger.trace("New configuration is committed")
    } yield ()
  }

  def onCommand[T](command: Command[T]): F[T] =
    errorLogging("Receiving Command") {
      command match {
        case command: ReadCommand[_] =>
          for {
            _      <- logger.trace(s"A read comment received ${command}")
            state_ <- getCurrentState
            result <- onReadCommand(state_, command)
          } yield result

        case command: WriteCommand[_] =>
          for {
            _        <- logger.trace(s"A write comment received ${command}")
            deferred <- emptyDeferred[T]
            state_   <- getCurrentState
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
            committed <- log.commitLogs(Map(nodeId -> (entry.index)))
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

      case ReplicateLog(peerId, term, nextIndex) =>
        background {
          errorLogging(s"Replicating logs to ${peerId}, Term: ${term}, NextIndex: ${nextIndex}") {
            for {
              response <- logReplicator.replicatedLogs(peerId, term, nextIndex)
              _        <- this.onReceive(response)
            } yield ()
          }
        }

      case StoreState =>
        storeState()

      case CommitLogs(matchIndex) =>
        for {
          committed <- log.commitLogs(matchIndex)
          _         <- if (committed) storeState() else Monad[F].unit
        } yield ()

      case AnnounceLeader(leaderId, true) =>
        leaderAnnouncer.reset() *> leaderAnnouncer.announce(leaderId)

      case AnnounceLeader(leaderId, false) =>
        logger.trace("Announcing a new leader without resetting ") *> leaderAnnouncer.announce(leaderId)

      case ResetLeaderAnnouncer =>
        leaderAnnouncer.reset()

    }

  private def storeState(): F[Unit] =
    for {
      _        <- logger.trace("Storing the new state in the storage")
      logState <- log.state
      node     <- getCurrentState
      _        <- storage.stateStorage.persistState(node.toPersistedState.copy(appliedIndex = logState.lastAppliedIndex))
    } yield ()

  private def runElection(): F[Unit] =
    for {
      _            <- delayElection()
      logState     <- log.state
      cluster      <- membershipManager.getClusterConfiguration
      currentState <- getCurrentState
      result  = currentState.onTimer(logState, cluster)
      actions = result._2
      _ <- setCurrentState(result._1)
      _ <- runActions(actions)
    } yield ()

  private def scheduleReplication(): F[Unit] =
    background {
      schedule(FiniteDuration(config.heartbeatIntervalMillis, TimeUnit.MILLISECONDS)) {
        for {
          node   <- getCurrentState
          config <- membershipManager.getClusterConfiguration
          actions = if (node.isInstanceOf[LeaderNode]) node.onReplicateLog(config) else List.empty
          _ <- runActions(actions)
        } yield ()
      }
    }

  private def scheduleElection(): F[Unit] =
    background {
      schedule(FiniteDuration(config.heartbeatTimeoutMillis, TimeUnit.MILLISECONDS)) {
        for {
          alive <- electionTimeoutElapsed
          _     <- if (alive) Monad[F].unit else runElection()
        } yield ()
      }
    }

}
