package raft4s

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, Timer}
import cats.implicits._
import cats.{Monad, MonadError, Parallel}
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
)(implicit ME: MonadError[F, Throwable]) {

  def start(): F[String] =
    for {
      server <- RpcServerBuilder[F].build(config.local, this)
      _      <- server.start()
      _      <- Timer[F].sleep(config.startElectionTimeout)
      node   <- state.get
      _      <- if (node.leader.isDefined) Monad[F].unit else runElection()
      _      <- scheduleElection()
      _      <- scheduleReplication()
      leader <- leaderAnnouncer.listen()
    } yield leader

  def listen(): F[String] =
    leaderAnnouncer.listen()

  def onReceive(msg: VoteRequest): F[VoteResponse] =
    for {
      logState <- log.state
      response <- state.modify(_.onReceive(logState, msg))
    } yield response

  def onReceive(msg: VoteResponse): F[Unit] =
    for {
      logState <- log.state
      actions  <- state.modify(_.onReceive(logState, msg))
      _        <- runActions(actions)
    } yield ()

  def onReceive(msg: AppendEntries): F[AppendEntriesResponse] =
    for {
      logState <- log.state
      current  <- state.get
      time     <- Timer[F].clock.monotonic(TimeUnit.SECONDS)
      _        <- lastHeartbeat.set(time)
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
      logState <- log.state
      actions  <- state.modify(_.onReceive(logState, msg))
      _        <- runActions(actions)
    } yield ()

  def onCommand[T](command: Command[T]): F[T] =
    command match {
      case command: ReadCommand[_] =>
        onReadCommand(command)

      case command: WriteCommand[_] =>
        for {
          deferred <- Deferred[F, T]
          state_   <- state.get
          actions  <- onWriteCommand(state_, command, deferred)
          _        <- runActions(actions)
          result   <- deferred.get
        } yield result
    }

  private def onReadCommand[T](command: ReadCommand[T]): F[T] =
    if (log.stateMachine.applyRead.isDefinedAt(command))
      log.stateMachine.applyRead(command).asInstanceOf[F[T]]
    else ME.raiseError(new RuntimeException("Can not run the command"))

  private def onWriteCommand[T](node: NodeState, command: WriteCommand[T], deferred: Deferred[F, T]): F[List[Action]] =
    node match {
      case LeaderNode(_, _, term, _, _) =>
        if (config.members.isEmpty)
          for {
            _ <- log.append(term, command, deferred)
            _ <- log.commitLogs(Map.empty, 0)
          } yield List.empty
        else
          for (_ <- log.append(term, command, deferred)) yield node.onReplicateLog()

      case _ =>
        for {
          leader   <- leaderAnnouncer.listen()
          client   <- clientProvider.client(leader)
          response <- client.send(command)
          _        <- deferred.complete(response)
        } yield List.empty
    }

  private def runActions(actions: List[Action]): F[Unit] =
    actions.parTraverse(action => runAction(action).attempt) *> Monad[F].unit

  private def runAction(action: Action): F[Unit] =
    action match {
      case RequestForVote(peerId, request) =>
        for {
          client   <- clientProvider.client(peerId)
          response <- client.send(request)
          res      <- this.onReceive(response)
        } yield res

      case ReplicateLog(peerId, term, sentLength) =>
        for {
          append   <- log.getAppendEntries(config.nodeId, term, sentLength)
          client   <- clientProvider.client(peerId)
          response <- client.send(append)
          _        <- this.onReceive(response)
        } yield ()

      case CommitLogs(ackedLength, minAckes) =>
        log.commitLogs(ackedLength, minAckes)

      case AnnounceLeader(leaderId) =>
        leaderAnnouncer.announce(leaderId)

      case ResetLeaderAnnouncer =>
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
      _    <- Timer[F].sleep(FiniteDuration(2000, TimeUnit.MILLISECONDS))
      node <- state.get
      actions = if (node.isInstanceOf[LeaderNode]) node.onReplicateLog() else List.empty
      _ <- runActions(actions)
    } yield ()

    Concurrent[F].start(Monad[F].foreverM(scheduled)) *> Monad[F].unit
  }

  private def scheduleElection(): F[Unit] = {
    val scheduled = for {
      _    <- Timer[F].sleep(FiniteDuration(5000, TimeUnit.MILLISECONDS))
      now  <- Timer[F].clock.monotonic(TimeUnit.SECONDS)
      lh   <- lastHeartbeat.get
      node <- state.get
      _    <- if (!node.isInstanceOf[LeaderNode] && now - lh > 5000) runElection() else Monad[F].unit
    } yield ()

    Concurrent[F].start(Monad[F].foreverM(scheduled)) *> Monad[F].unit
  }
}

object Raft {
  def make[F[_]: Monad: Concurrent: Parallel: Timer: RpcClientBuilder: RpcServerBuilder](
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
