lazy val Version           = "0.0.3"
lazy val ScalaVersion      = "2.13.10"
lazy val CatsEffectVersion = "3.4.6"
lazy val OdinVersion       = "0.13.0"
lazy val ScalaTestVersion  = "3.2.15"
lazy val RocksDbVersion    = "6.6.4"

lazy val CatsEffect = "org.typelevel" %% "cats-effect" % CatsEffectVersion

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
      "com.github.valskalla" %% "odin-core" % OdinVersion,
      "org.scalatest"        %% "scalatest" % ScalaTestVersion % Test
    )
  )

lazy val effect = (project in file("raft4s-effect"))
  .settings(GlobalSettingsGroup)
  .settings(
    name := "raft4s-effect",
    libraryDependencies ++= Seq(
      CatsEffect,
      "com.github.valskalla" %% "odin-core"   % OdinVersion,
      "org.scalatest"        %% "scalatest"   % ScalaTestVersion % Test
    )
  )
  .dependsOn(core)
  .aggregate(core)

lazy val future = (project in file("raft4s-future"))
  .settings(GlobalSettingsGroup)
  .settings(
    name := "raft4s-future",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % ScalaTestVersion % Test
    )
  )
  .dependsOn(core)
  .aggregate(core)

lazy val grpc = (project in file("raft4s-grpc"))
  .settings(GlobalSettingsGroup)
  .settings(
    name := "raft4s-grpc",
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    ),
    libraryDependencies ++= Seq(
      CatsEffect,
      "com.thesamet.scalapb" %% "scalapb-runtime"      % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
      "io.grpc"               % "grpc-netty"           % scalapb.compiler.Version.grpcJavaVersion,
      "io.grpc"               % "grpc-services"        % scalapb.compiler.Version.grpcJavaVersion
    )
  )
  .dependsOn(core)
  .aggregate(core)

lazy val rocksdb = (project in file("raft4s-rocksdb"))
  .settings(GlobalSettingsGroup)
  .settings(
    name := "raft4s-rocksdb",
    libraryDependencies ++= Seq(
      CatsEffect,
      "org.rocksdb" % "rocksdbjni" % RocksDbVersion
    )
  )
  .dependsOn(core)
  .aggregate(core)

lazy val root = (project in file("."))
  .aggregate(rocksdb, core, effect, grpc, future)
  .settings(GlobalSettingsGroup)
  .settings(
    name := "raft4s",
    moduleName := "raft4s",
    publish := {},
    publishLocal := {}
  )
