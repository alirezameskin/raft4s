package raft4s.protocol

import scala.collection.immutable.Set

trait ClusterConfiguration {
  def members: Set[String]
  def quorumReached(nodes: Set[String]): Boolean
}

case class NewClusterConfiguration(members: Set[String]) extends ClusterConfiguration {

  private val quorum = (members.size / 2) + 1

  override def quorumReached(nodes: Set[String]): Boolean =
    nodes.intersect(members).size >= quorum
}

case class JointClusterConfiguration(oldMembers: Set[String], newMembers: Set[String]) extends ClusterConfiguration {

  private val oldQuorum = (oldMembers.size / 2) + 1
  private val newQuorum = (newMembers.size / 2) + 1

  override def members: Set[String] =
    oldMembers ++ newMembers

  override def quorumReached(nodes: Set[String]): Boolean =
    (nodes.intersect(oldMembers).size >= oldQuorum) && (nodes.intersect(newMembers).size >= newQuorum)
}
