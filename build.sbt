lazy val Version           = "0.0.2"
lazy val ScalaVersion      = "2.13.4"
lazy val CatsEffectVersion = "2.3.0"
lazy val OdinVersion       = "0.9.1"
lazy val ScalaTestVersion  = "3.2.0"

val GlobalSettingsGroup: Seq[Setting[_]] = Seq(
  version := Version,
  scalaVersion := ScalaVersion,
  homepage := Some(url("https://github.com/alirezameskin/raft4s")),
  organization := "com.github.alirezameskin",
  githubOwner := "alirezameskin",
  githubRepository := "raft4s",
  githubTokenSource := TokenSource.Or(TokenSource.Environment("GITHUB_TOKEN"), TokenSource.GitConfig("github.token"))
)

lazy val core = (project in file("raft4s-core"))
  .settings(GlobalSettingsGroup)
  .settings(
    name := "raft4s-core",
    libraryDependencies ++= Seq(
      "org.typelevel"        %% "cats-effect" % CatsEffectVersion,
      "com.github.valskalla" %% "odin-core"   % OdinVersion,
      "org.scalatest"        %% "scalatest"   % ScalaTestVersion % Test
    )
  )

lazy val grpc = (project in file("raft4s-grpc"))
  .settings(GlobalSettingsGroup)
  .settings(
    name := "raft4s-grpc",
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value / "scalapb"
    ),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime"      % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
      "io.grpc"               % "grpc-netty"           % scalapb.compiler.Version.grpcJavaVersion,
      "io.grpc"               % "grpc-services"        % scalapb.compiler.Version.grpcJavaVersion
    )
  )
  .dependsOn(core)
  .aggregate(core)

lazy val demo = (project in file("raft4s-demo"))
  .settings(GlobalSettingsGroup)
  .settings(
    name := "raft4s-demo",
    publish := {},
    publishLocal := {}
  )
  .dependsOn(core, grpc)
  .aggregate(core, grpc)

lazy val root = (project in file("."))
  .aggregate(demo)
  .settings(GlobalSettingsGroup)
  .settings(
    name := "raft4s",
    moduleName := "raft4s",
    publish := {},
    publishLocal := {}
  )
