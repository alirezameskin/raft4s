package raft4s

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, Sync, Timer}
import cats.implicits._
import cats.{Monad, MonadError, Parallel}
import io.odin.Logger
import raft4s.log.ReplicatedLog
import raft4s.node._
import raft4s.protocol._
import raft4s.rpc._
import raft4s.storage.Storage

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class Raft[F[_]: Monad: Concurrent: Timer: Parallel: RpcServerBuilder](
  val config: Configuration,
  val clientProvider: RpcClientProvider[F],
  val leaderAnnouncer: LeaderAnnouncer[F],
  val log: ReplicatedLog[F],
  val storage: Storage[F],
  val state: Ref[F, NodeState],
  val lastHeartbeat: Ref[F, Long]
)(implicit ME: MonadError[F, Throwable], logger: Logger[F]) {

  def start(): F[String] =
    for {
      _      <- logger.trace("Cluster is starting")
      server <- RpcServerBuilder[F].build(config.local, this)
      _      <- server.start()
      _      <- logger.debug("RPC server started")
      delay  <- electionDelay()
      _      <- logger.trace(s"Delay to start the election ${delay}")
      _      <- Timer[F].sleep(delay)
      node   <- state.get
      _      <- if (node.leader.isDefined) Monad[F].unit else runElection()
      _      <- scheduleElection()
      _      <- scheduleReplication()
      _      <- logger.trace("Waiting for the leader to be elected.")
      leader <- leaderAnnouncer.listen()
      _      <- logger.info(s"A Leader is elected. Leader: '${leader}'")
    } yield leader

  def listen(): F[String] =
    leaderAnnouncer.listen()

  def onReceive(msg: VoteRequest): F[VoteResponse] =
    for {
      _        <- logger.trace(s"A Vote request received from ${msg.nodeId}, Term: ${msg.logTerm}")
      logState <- log.state
      response <- state.modify(_.onReceive(logState, msg))
      _        <- logger.trace(s"Vote response to the request ${response}")
    } yield response

  def onReceive(msg: VoteResponse): F[Unit] =
    for {
      _        <- logger.trace(s"A Vote response received from ${msg.nodeId}, Granted: ${msg.granted}")
      logState <- log.state
      actions  <- state.modify(_.onReceive(logState, msg))
      _        <- runActions(actions)
    } yield ()

  def onReceive(msg: AppendEntries): F[AppendEntriesResponse] =
    for {
      _        <- logger.trace(s"A AppendEntries request received from ${msg.leaderId}, contains ${msg.entries.size} entries")
      logState <- log.state
      current  <- state.get
      time     <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
      _        <- lastHeartbeat.set(time)
      _        <- logger.trace("Updating the last heartbeat")

      (nextState, (response, actions)) = current.onReceive(logState, msg)

      _ <- runActions(actions)
      _ <-
        if (response.success)
          log.appendEntries(msg.entries, msg.logLength, msg.leaderCommit) *> state.set(nextState)
        else
          Monad[F].unit
    } yield response

  def onReceive(msg: AppendEntriesResponse): F[Unit] =
    for {
      _        <- logger.trace(s"A AppendEntriesResponse received from ${msg.nodeId}. ${msg}")
      logState <- log.state
      actions  <- state.modify(_.onReceive(logState, msg))
      _        <- runActions(actions)
    } yield ()

  def onCommand[T](command: Command[T]): F[T] =
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

  private def onReadCommand[T](node: NodeState, command: ReadCommand[T]): F[T] =
    node match {
      case _: LeaderNode =>
        for {
          _ <- logger.trace("Current node is the leader, it is running the read command")
          res <-
            if (log.stateMachine.applyRead.isDefinedAt(command)) log.stateMachine.applyRead(command).asInstanceOf[F[T]]
            else ME.raiseError(new RuntimeException("Can not run the command"))
        } yield res

      case _: FollowerNode if config.followerAcceptRead =>
        for {
          _ <- logger.trace("Current node is a follower, it is running the read command")
          res <-
            if (log.stateMachine.applyRead.isDefinedAt(command))
              log.stateMachine.applyRead(command).asInstanceOf[F[T]]
            else ME.raiseError(new RuntimeException("Can not run the command"))
        } yield res

      case _ =>
        for {
          _        <- logger.trace("Read command has to be ran on the leader node")
          leader   <- leaderAnnouncer.listen()
          _        <- logger.trace(s"The current leader is ${leader}")
          client   <- clientProvider.client(leader)
          response <- client.send(command)
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
          client   <- clientProvider.client(leader)
          response <- client.send(command)
          _        <- logger.trace("Response for the write command received from the leader")
          _        <- deferred.complete(response)
        } yield List.empty
    }

  private def runActions(actions: List[Action]): F[Unit] =
    actions.parTraverse(action => runAction(action).attempt) *> Monad[F].unit

  private def runAction(action: Action): F[Unit] =
    action match {
      case RequestForVote(peerId, request) =>
        for {
          _        <- logger.trace(s"Sending a vote rerquest to ${peerId}. Request: ${request}")
          client   <- clientProvider.client(peerId)
          response <- client.send(request)
          res      <- this.onReceive(response)
        } yield res

      case ReplicateLog(peerId, term, sentLength) =>
        for {
          _        <- logger.trace(s"Sending AppendEntries request to to ${peerId}. Term: ${term}")
          append   <- log.getAppendEntries(config.nodeId, term, sentLength)
          client   <- clientProvider.client(peerId)
          response <- client.send(append)
          _        <- this.onReceive(response)
        } yield ()

      case CommitLogs(ackedLength, minAckes) =>
        log.commitLogs(ackedLength, minAckes)

      case AnnounceLeader(leaderId, true) =>
        logger.trace(s"Announcing a new leader is elected, New leader: ${leaderId} ") *>
          leaderAnnouncer.reset() *> leaderAnnouncer.announce(leaderId)

      case AnnounceLeader(leaderId, false) =>
        logger.trace(s"Announcing a new leader is elected, New leader: ${leaderId} ") *>
          leaderAnnouncer.announce(leaderId)

      case ResetLeaderAnnouncer =>
        logger.trace("Resetting the leader announcer for the new leader.") *>
          leaderAnnouncer.reset()
    }

  private def runElection(): F[Unit] =
    for {
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

    Concurrent[F].start(Monad[F].foreverM(scheduled)) *> Monad[F].unit
  }

  private def scheduleElection(): F[Unit] = {
    val scheduled = for {
      _    <- Timer[F].sleep(FiniteDuration(config.heartbeatTimeoutMillis, TimeUnit.MILLISECONDS))
      now  <- Timer[F].clock.monotonic(TimeUnit.MILLISECONDS)
      lh   <- lastHeartbeat.get
      node <- state.get
      _    <- if (!node.isInstanceOf[LeaderNode] && now - lh > config.heartbeatTimeoutMillis) runElection() else Monad[F].unit
    } yield ()

    Concurrent[F].start(Monad[F].foreverM(scheduled)) *> Monad[F].unit
  }

  private def electionDelay(): F[FiniteDuration] = Sync[F].delay {
    val delay =
      config.electionMinDelayMillis + scala.util.Random.nextInt(config.electionMaxDelayMillis - config.electionMinDelayMillis)

    FiniteDuration(delay, TimeUnit.MILLISECONDS)
  }
}

object Raft {
  def make[F[_]: Monad: Concurrent: Parallel: Timer: RpcClientBuilder: RpcServerBuilder: Logger](
    config: Configuration,
    storage: Storage[F],
    stateMachine: StateMachine[F]
  ): F[Raft[F]] =
    for {
      persistedState  <- storage.retrievePersistedState()
      clientProvider  <- RpcClientProvider.build[F](config.members)
      leaderAnnouncer <- LeaderAnnouncer.build[F]
      nodeState       <- Ref.of[F, NodeState](FollowerNode(config.nodeId, config.nodes, persistedState.term, persistedState.votedFor))
      heartbeat       <- Ref.of[F, Long](0L)
      replicateLog = ReplicatedLog.build[F](storage.log, stateMachine)

    } yield new Raft[F](config, clientProvider, leaderAnnouncer, replicateLog, storage, nodeState, heartbeat)
}
