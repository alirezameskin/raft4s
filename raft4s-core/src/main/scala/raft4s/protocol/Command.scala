package raft4s.protocol

sealed trait Command[OUT] extends Serializable
trait ReadCommand[OUT]    extends Command[OUT]
trait WriteCommand[OUT]   extends Command[OUT]

sealed trait ClusterConfigurationCommand                                        extends Command[Unit]
case class JointConfiguration(oldMembers: Set[String], newMembers: Set[String]) extends ClusterConfigurationCommand
case class NewConfiguration(members: Set[String])                               extends ClusterConfigurationCommand
