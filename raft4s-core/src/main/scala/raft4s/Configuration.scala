package raft4s

case class Configuration(
  local: Node,
  members: Seq[Node] = List.empty,
  followerAcceptRead: Boolean = true,
  logCompactionThreshold: Int = 100,
  electionMinDelayMillis: Int = 150,
  electionMaxDelayMillis: Int = 300,
  heartbeatIntervalMillis: Int = 2000,
  heartbeatTimeoutMillis: Int = 6000
) {
  def nodeId: String      = local.toString
  def nodes: List[String] = local.toString :: members.map(_.toString).toList
}
