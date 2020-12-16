package raft4s.protocol

import scala.collection.immutable.Set

trait ClusterConfiguration {
  def members: Set[String]
}

case class NewClusterConfiguration(members: Set[String]) extends ClusterConfiguration
