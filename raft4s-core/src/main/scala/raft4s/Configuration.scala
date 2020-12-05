package raft4s

case class Address(host: String, port: Int) {
  override def toString: String = id
  def id: String                = s"${host}:${port}"
}

case class Configuration(
  local: Address,
  members: Seq[Address] = List.empty,
  followerAcceptRead: Boolean = true,
  electionMinDelayMillis: Int = 150,
  electionMaxDelayMillis: Int = 300,
  heartbeatIntervalMillis: Int = 2000,
  heartbeatTimeoutMillis: Int = 6000
) {
  def nodeId: String      = local.toString
  def nodes: List[String] = local.toString :: members.map(_.toString).toList
}
