package raft4s.internal

import raft4s.Node

private[raft4s] trait LeaderAnnouncer[F[_]] {

  def announce(leader: Node): F[Unit]

  def reset(): F[Unit]

  def listen(): F[Node]
}
