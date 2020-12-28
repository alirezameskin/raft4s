package raft4s.effect.storage.memory

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import raft4s.LogEntry
import raft4s.storage.LogStorage

class MemoryLogStorage[F[_]: Sync](itemsRef: Ref[F, Map[Long, LogEntry]], lastIndexRef: Ref[F, Long]) extends LogStorage[F] {

  override def lastIndex: F[Long] =
    lastIndexRef.get

  override def get(index: Long): F[LogEntry] =
    itemsRef.get.map(_.get(index).orNull)

  override def put(index: Long, logEntry: LogEntry): F[LogEntry] =
    for {
      _ <- itemsRef.update(_ + (index -> logEntry))
      _ <- lastIndexRef.update(i => Math.max(i, index))
    } yield logEntry

  override def deleteBefore(index: Long): F[Unit] =
    for {
      _ <- itemsRef.update(items => items.filter(_._1 >= index))
    } yield ()

  override def deleteAfter(index: Long): F[Unit] =
    for {
      _ <- itemsRef.update(items => items.filter(_._1 <= index))
    } yield ()

}

object MemoryLogStorage {
  def empty[F[_]: Sync]: F[MemoryLogStorage[F]] =
    for {
      items <- Ref.of[F, Map[Long, LogEntry]](Map.empty)
      last  <- Ref.of[F, Long](0L)
    } yield new MemoryLogStorage[F](items, last)
}
