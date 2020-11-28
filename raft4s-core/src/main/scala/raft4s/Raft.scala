package raft4s

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{ContextShift, IO}
import cats.implicits._
import raft4s.log.ReplicatedLog
import raft4s.node.{LeaderNode, NodeState}
import raft4s.rpc._

class Raft(val nodeId: String, val members: Map[String, RpcClient], val log: ReplicatedLog, val state: Ref[IO, NodeState])(
  implicit CS: ContextShift[IO]
) {

  def start(): IO[Unit] =
    for {
      logState <- log.state
      actions  <- state.modify(_.onTimer(logState))
      _        <- runActions(actions)
    } yield ()

  def onReceive(msg: VoteRequest): IO[VoteResponse] =
    for {
      logState <- log.state
      response <- state.modify(_.onReceive(logState, msg))
    } yield response

  def onReceive(msg: VoteResponse): IO[Unit] =
    for {
      logState <- log.state
      actions  <- state.modify(_.onReceive(logState, msg))
      _        <- runActions(actions)
    } yield ()

  def onReceive(msg: AppendEntries): IO[AppendEntriesResponse] =
    for {
      logState <- log.state
      result   <- state.modify(_.onReceive(logState, msg))
      _        <- if (result.success) log.appendEntries(msg.entries, msg.logLength, msg.leaderCommit) else IO(())
    } yield result

  def onReceive(msg: AppendEntriesResponse): IO[Unit] =
    for {
      logState <- log.state
      actions  <- state.modify(_.onReceive(logState, msg))
      _        <- runActions(actions)
    } yield ()

  def onCommand[T](command: Command[T]): IO[T] =
    for {
      deferred <- Deferred[IO, T]
      state_   <- state.get
      actions  <- onCommand(state_, command, deferred)
      _        <- runActions(actions)
      result   <- deferred.get
    } yield result

  private def onCommand[T](node: NodeState, command: Command[T], deferred: Deferred[IO, T]): IO[List[Action]] =
    node match {
      case LeaderNode(_, _, currentTerm, _, _) =>
        for {
          _       <- log.append(currentTerm, command, deferred)
          actions <- IO(node.onReplicateLog())
        } yield actions

      case _ =>
        IO.raiseError(new RuntimeException("Only leader node can run commands"))
    }

  private def runActions(actions: List[Action]): IO[Unit] =
    actions.parTraverse(a => runAction(a)) *> IO.unit

  private def runAction(action: Action): IO[Unit] =
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
        IO(println("Start election timer"))

      case CancelElectionTimer =>
        IO(println("Cancel election"))
    }
}
