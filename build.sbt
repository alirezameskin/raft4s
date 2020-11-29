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

lazy val demo = (project in file("raft4s-demo"))
  .settings(
    scalaVersion := "2.13.4",
    name := "raft4s-demo"
  )
  .dependsOn(core)
  .aggregate(core)

lazy val root = (project in file("."))
  .aggregate(demo)
  .settings(
    scalaVersion := "2.13.4",
    moduleName := "raft4s"
  )
