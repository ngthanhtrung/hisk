import Dependencies._

lazy val buildSettings = Seq(
  organization := "com.ngthanhtrung",
  version := "0.0.1",
  scalaVersion := "2.11.8"
)

lazy val deps = Seq(
)

lazy val hisk = (project in file("."))
  .settings(buildSettings: _*)
  .settings(
    libraryDependencies ++= deps
  )
