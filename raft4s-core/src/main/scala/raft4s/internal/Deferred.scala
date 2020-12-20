package raft4s.internal

abstract class Deferred[F[_], A] {

  def get: F[A]

  def complete(a: A): F[Unit]
}
