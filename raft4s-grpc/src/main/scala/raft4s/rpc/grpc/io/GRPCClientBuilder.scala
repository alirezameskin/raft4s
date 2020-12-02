package raft4s.rpc.grpc.io

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import _root_.io.grpc.ManagedChannelBuilder
import cats.effect.IO
import com.google.protobuf.ByteString
import raft4s.Address
import raft4s.grpc.protos
import raft4s.protocol.{AppendEntries, AppendEntriesResponse, VoteRequest, VoteResponse}
import raft4s.rpc.{RpcClient, RpcClientBuilder}

class GRPCClientBuilder extends RpcClientBuilder[IO] {
  override def build(address: Address): RpcClient[IO] = {

    implicit val EC = scala.concurrent.ExecutionContext.global
    implicit val CS = IO.contextShift(scala.concurrent.ExecutionContext.global)

    val channel = ManagedChannelBuilder.forAddress(address.host, address.port).usePlaintext().build()
    val stub    = protos.RaftGrpc.stub(channel)

    new RpcClient[IO] {
      override def send(voteRequest: VoteRequest): IO[VoteResponse] = {
        val request =
          protos.VoteRequest(voteRequest.nodeId, voteRequest.currentTerm, voteRequest.logLength, voteRequest.logTerm)

        val response = stub.vote(request).map(res => VoteResponse(res.nodeId, res.term, res.granted))

        IO.fromFuture(IO(response))
      }

      override def send(appendEntries: AppendEntries): IO[AppendEntriesResponse] = {
        val request = protos.AppendEntriesRequest(
          appendEntries.leaderId,
          appendEntries.term,
          appendEntries.logLength,
          appendEntries.logTerm,
          appendEntries.leaderCommit,
          appendEntries.entries.map { entry =>
            val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
            val oos                           = new ObjectOutputStream(stream)
            oos.writeObject(entry.command)
            oos.close
            protos.LogEntry(entry.term, entry.index, ByteString.copyFrom(stream.toByteArray))
          }
        )

        val response =
          stub.appendEntries(request).map(res => AppendEntriesResponse(res.nodeId, res.currentTerm, res.ack, res.success))

        IO.fromFuture(IO(response))
      }
    }
  }
}
