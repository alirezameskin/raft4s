package raft4s.protocol

import raft4s.Node

sealed trait Command[OUT] extends Serializable
trait ReadCommand[OUT]    extends Command[OUT]
trait WriteCommand[OUT]   extends Command[OUT]

sealed private[raft4s] trait ClusterConfigurationCommand extends WriteCommand[Unit] {
  def toConfig: ClusterConfiguration
}

private[raft4s] case class JointConfigurationCommand(oldMembers: Set[Node], newMembers: Set[Node])
    extends ClusterConfigurationCommand {
  override def toConfig: ClusterConfiguration = JointClusterConfiguration(oldMembers, newMembers)
}

private[raft4s] case class NewConfigurationCommand(members: Set[Node]) extends ClusterConfigurationCommand {
  override def toConfig: ClusterConfiguration = NewClusterConfiguration(members)
}
