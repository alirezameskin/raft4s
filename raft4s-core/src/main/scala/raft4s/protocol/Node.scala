package raft4s.protocol

case class Node(host: String, port: Int) {
  override def toString: String = id
  def id: String                = s"${host}:${port}"
}

object Node {
  def fromString(str: String): Option[Node] = {
    str.split(":") match {
      case Array(host, ip) => Some(Node(host, ip.toInt))
      case _ => None
    }
  }
}
