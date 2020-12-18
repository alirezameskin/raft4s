package raft4s.protocol

import raft4s.Node

import scala.collection.immutable.Set

trait ClusterConfiguration {
  def members: Set[Node]
  def quorumReached(nodes: Set[Node]): Boolean
}

case class NewClusterConfiguration(members: Set[Node]) extends ClusterConfiguration {

  private val quorum = (members.size / 2) + 1

  override def quorumReached(nodes: Set[Node]): Boolean = {
    val res = nodes.intersect(members).size >= quorum
    println(s"Reached quorum ? ${members} ${res}")

    res

  }
}

case class JointClusterConfiguration(oldMembers: Set[Node], newMembers: Set[Node]) extends ClusterConfiguration {

  private val oldQuorum = (oldMembers.size / 2) + 1
  private val newQuorum = (newMembers.size / 2) + 1

  override def members: Set[Node] =
    oldMembers ++ newMembers

  override def quorumReached(nodes: Set[Node]): Boolean = {
    val res = (nodes.intersect(oldMembers).size >= oldQuorum) && (nodes.intersect(newMembers).size >= newQuorum)
    println(s"Reached quorum ? ${oldMembers} ${newMembers} ? ${res}")

    res
  }
}
