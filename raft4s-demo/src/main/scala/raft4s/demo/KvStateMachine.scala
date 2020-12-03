package raft4s.demo

import java.util.HashMap

import cats.effect.IO
import raft4s.StateMachine
import raft4s.protocol.{ReadCommand, WriteCommand}

case class Put(key: String, value: String) extends WriteCommand[String]
case class Get(key: String)                extends ReadCommand[String]

class KvStateMachine extends StateMachine[IO] {
  private var map             = new HashMap[String, String]()
  private var lastIndex: Long = 0

  override def applyWrite: PartialFunction[(Long, WriteCommand[_]), IO[Any]] = { case (index, Put(key, value)) =>
    IO {
      map.put(key, value)
      lastIndex = index
      value
    }
  }

  override def applyRead: PartialFunction[ReadCommand[_], IO[Any]] = { case Get(key) =>
    IO.pure(map.get(key))
  }
}
