package raft4s

import java.util.concurrent.TimeUnit

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, Timer}
import cats.implicits._
import cats.{Monad, MonadError, Parallel}
import raft4s.log.ReplicatedLog
import raft4s.node.{FollowerNode, LeaderNode, NodeState}
import raft4s.protocol.{AppendEntries, AppendEntriesResponse, Command, VoteRequest, VoteResponse}
import raft4s.rpc._
import raft4s.storage.Storage

import scala.concurrent.duration.FiniteDuration

class Raft[F[_]: Monad: Concurrent: Timer: Parallel](
  val nodeId: String,
  val members: collection.Map[String, RpcClient[F]],
  val log: ReplicatedLog[F],
  val storage: Storage[F],
  val state: Ref[F, NodeState],
  val lastHeartbeat: Ref[F, Long]
)(implicit
  ME: MonadError[F, Throwable]
) {

  def start(): F[Unit] =
    for {
      _ <- runElection()
      _ <- scheduleElection()
      _ <- scheduleReplication()
    } yield ()

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
      (nextState, response) = current.onReceive(logState, msg)
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
    for {
      deferred <- Deferred[F, T]
      state_   <- state.get
      actions  <- onCommand(state_, command, deferred)
      _        <- runActions(actions)
      result   <- deferred.get
    } yield result

  private def onCommand[T](node: NodeState, command: Command[T], deferred: Deferred[F, T]): F[List[Action]] =
    node match {
      case LeaderNode(_, _, term, _, _) =>
        if (members.isEmpty)
          for {
            _ <- log.append(term, command, deferred)
            _ <- log.commitLogs(Map.empty, 0)
          } yield List.empty
        else
          for (_ <- log.append(term, command, deferred)) yield node.onReplicateLog()

      case _ =>
        ME.raiseError(new RuntimeException("Only leader node can run commands"))
    }

  private def runActions(actions: List[Action]): F[Unit] =
    actions.parTraverse(a => runAction(a)) *> Monad[F].unit

  private def runAction(action: Action): F[Unit] =
    action match {
      case RequestForVote(peerId, request) =>
        for {
          response <- members(peerId).send(request)
          res      <- this.onReceive(response)
        } yield res

      case ReplicateLog(peerId, term, sentLength) =>
        for {
          append   <- log.getAppendEntries(nodeId, term, sentLength)
          response <- members(peerId).send(append)
          _        <- this.onReceive(response)
        } yield ()

      case CommitLogs(ackedLength, minAckes) =>
        log.commitLogs(ackedLength, minAckes)
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
  def make[F[_]: Monad: Concurrent: Parallel: Timer: RpcClientBuilder](
    config: Configuration,
    storage: Storage[F],
    stateMachine: StateMachine[F]
  ): F[Raft[F]] =
    for {
      persistedState <- storage.retrievePersistedState()
      nodeId  = config.local.id
      nodes   = config.local.id :: config.members.map(_.id).toList
      builder = implicitly[RpcClientBuilder[F]]
      clients = config.members.map(s => (s.id, builder.build(s))).toMap
      nodeState <- Ref.of[F, NodeState](FollowerNode(nodeId, nodes, persistedState.term, persistedState.votedFor))
      heartbeat <- Ref.of[F, Long](0L)
      replicateLog = ReplicatedLog.build[F](storage.log, stateMachine)

    } yield new Raft[F](nodeId, clients, replicateLog, storage, nodeState, heartbeat)
}
