name := "raft4s"
version := "0.1"
scalaVersion := "2.13.4"

lazy val core = (project in file("raft4s-core"))
  .settings(
    scalaVersion := "2.13.4",
    name := "raft4s-core",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "2.2.0",
      "org.scalatest" %% "scalatest"   % "3.2.0" % Test
    )
  )

lazy val grpc = (project in file("raft4s-grpc"))
  .settings(
    scalaVersion := "2.13.4",
    name := "raft4s-grpc",
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value / "scalapb"
    ),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime"      % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
      "io.grpc"               % "grpc-netty"           % scalapb.compiler.Version.grpcJavaVersion
    )
  )
  .dependsOn(core)
  .aggregate(core)

lazy val demo = (project in file("raft4s-demo"))
  .settings(
    scalaVersion := "2.13.4",
    name := "raft4s-demo"
  )
  .dependsOn(core, grpc)
  .aggregate(core, grpc)

lazy val root = (project in file("."))
  .aggregate(demo)
  .settings(
    scalaVersion := "2.13.4",
    moduleName := "raft4s"
  )
