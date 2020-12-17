package raft4s.protocol

sealed trait Command[OUT] extends Serializable
trait ReadCommand[OUT]    extends Command[OUT]
trait WriteCommand[OUT]   extends Command[OUT]

sealed trait ClusterConfigurationCommand extends WriteCommand[Unit] {
  def toConfig: ClusterConfiguration
}

case class JointConfigurationCommand(oldMembers: Set[String], newMembers: Set[String]) extends ClusterConfigurationCommand {
  override def toConfig: ClusterConfiguration = JointClusterConfiguration(oldMembers, newMembers)
}
case class NewConfigurationCommand(members: Set[String]) extends ClusterConfigurationCommand {
  override def toConfig: ClusterConfiguration = NewClusterConfiguration(members)
}
