package raft4s

import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._
import cats.{Monad, MonadError, Parallel}
import raft4s.log.ReplicatedLog
import raft4s.node.{LeaderNode, NodeState}
import raft4s.rpc._

class Raft[F[_]: Monad: Concurrent: Parallel](
  val nodeId: String,
  val members: Map[String, RpcClient[F]],
  val log: ReplicatedLog[F],
  val state: Ref[F, NodeState]
)(implicit
  ME: MonadError[F, Throwable]
) {

  def start(): F[Unit] =
    for {
      logState <- log.state
      actions  <- state.modify(_.onTimer(logState))
      _        <- runActions(actions)
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
      case LeaderNode(_, _, currentTerm, _, _) =>
        for {
          _       <- log.append(currentTerm, command, deferred)
          actions <- Monad[F].pure(node.onReplicateLog())
        } yield actions

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

      case StartElectionTimer =>
        Monad[F].pure(println("Start election timer"))

      case CancelElectionTimer =>
        Monad[F].pure(println("Cancel election"))
    }
}
