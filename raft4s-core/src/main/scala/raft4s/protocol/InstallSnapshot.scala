package raft4s.protocol

import raft4s.LogEntry
import raft4s.storage.Snapshot

case class InstallSnapshot(snapshot: Snapshot, lastEntry: LogEntry)
