package raft4s

import scala.concurrent.duration.FiniteDuration

case class Address(host: String, port: Int) {
  override def toString: String = id
  def id: String                = s"${host}:${port}"
}

case class Configuration(local: Address, members: Seq[Address], startElectionTimeout: FiniteDuration) {
  def nodeId: String      = local.toString
  def nodes: List[String] = local.toString :: members.map(_.toString).toList
}
