package raft4s.rpc.grpc.serializer

import com.google.protobuf.ByteString

trait Serializer {

  def encode[T](obj: T): ByteString

  def decode[T](byteString: ByteString): T
}
