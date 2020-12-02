package raft4s

case class Address(host: String, port: Int)

case class Server(id: String, address: Address)

case class Configuration(local: Server, members: Seq[Server])
