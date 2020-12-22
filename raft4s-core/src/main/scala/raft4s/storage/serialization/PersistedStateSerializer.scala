package raft4s.storage.serialization

import raft4s.Node
import raft4s.storage.PersistedState

import java.nio.charset.StandardCharsets
import scala.util.Try

class PersistedStateSerializer extends Serializer[PersistedState] {

  override def toBytes(state: PersistedState): Array[Byte] =
    List(state.term.toString, state.votedFor.map(_.id).getOrElse(""), state.appliedIndex.toString)
      .mkString("\n")
      .getBytes

  override def fromBytes(bytes: Array[Byte]): Option[PersistedState] = {
    val result = for {
      str          <- Try(new String(bytes, StandardCharsets.UTF_8))
      lines        <- Try(str.linesIterator.toList)
      term         <- Try(lines.head.toLong)
      voted        <- Try(lines.tail.headOption)
      appliedIndex <- Try(lines.tail.tail.headOption.map(_.toLong).getOrElse(0L))
    } yield PersistedState(term, voted.flatMap(Node.fromString), appliedIndex)

    result.toOption
  }
}
