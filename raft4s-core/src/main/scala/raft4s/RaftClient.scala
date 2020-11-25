package raft4s

import cats.effect.{ContextShift, IO}
import raft4s.rpc.{ReadCommand, WriteCommand}

trait RaftClient[F[_]] {

  def write(command: WriteCommand[String]): F[String]

  def read(command: ReadCommand[String]): F[String]
}

object RaftClient {
  def apply(raft: Raft)(implicit CS: ContextShift[IO]): RaftClient[IO] = new RaftClient[IO]() {

    override def write(command: WriteCommand[String]): IO[String] = {
      IO(println(s"write command ${command}")) *>
      raft.onCommand(command)
    }

    override def read(command: ReadCommand[String]): IO[String] = {
      IO(println(s"Read command ${command}")) *> ???
    }
  }
}
