package raft4s.demo

import cats.effect.{Blocker, ExitCode, IO, IOApp, Resource}
import fs2.{io, text, Stream}
import java.nio.file.Paths

object Converter extends IOApp {

  val converter: Stream[IO, Unit] = Stream.resource(Blocker[IO]).flatMap { blocker =>
    def fahrenheitToCelsius(f: Double): Double =
      (f - 32.0) * (5.0 / 9.0)

    io.file
      .readAll[IO](Paths.get("testdata/fahrenheit.txt"), blocker, 4096)
      .through(text.utf8Decode)
      .through(text.lines)
      .filter(s => !s.trim.isEmpty && !s.startsWith("//"))
      .map(line => fahrenheitToCelsius(line.toDouble).toString)
      .intersperse("\n")
      .through(text.utf8Encode)
      .through(io.file.writeAll(Paths.get("testdata/celsius.txt"), blocker))
  }

  def run(args: List[String]): IO[ExitCode] =
    converter.compile.drain.as(ExitCode.Success)
}
