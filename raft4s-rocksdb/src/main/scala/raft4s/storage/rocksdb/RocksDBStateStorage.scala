package raft4s.storage.rocksdb

import cats.MonadError
import cats.effect.Sync
import cats.implicits._
import io.odin.Logger
import org.rocksdb.ColumnFamilyHandle
import org.{rocksdb => jrocks}
import raft4s.storage.rocksdb.serializer.ObjectSerializer
import raft4s.storage.{PersistedState, StateStorage}

import scala.util.Try

class RocksDBStateStorage[F[_]: Sync: Logger](db: jrocks.RocksDB, handle: ColumnFamilyHandle)(implicit
  ME: MonadError[F, Throwable]
) extends StateStorage[F] {

  private val NODE_STATE_KEY = "latest_state".getBytes

  override def persistState(state: PersistedState): F[Unit] =
    for {
      _     <- Logger[F].debug(s"Persisting the state ${state}")
      value <- ME.fromTry(Try(ObjectSerializer.encode(state)))
      _     <- ME.fromTry(Try(db.put(handle, NODE_STATE_KEY, value)))
    } yield ()

  override def retrieveState(): F[Option[PersistedState]] =
    for {
      _      <- Logger[F].debug("Retrieving the persisted state")
      bytes  <- ME.fromTry(Try(db.get(handle, NODE_STATE_KEY)))
      result <- ME.fromTry(Try(Option(bytes).map(ObjectSerializer.decode[PersistedState])))
      _      <- Logger[F].debug(s"Retrieved state ${result}")
    } yield result
}
