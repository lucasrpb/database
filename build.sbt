organization := "services.scalable"
name := "database"

version := "master"

scalaVersion := "2.13.6"

val jacksonVersion = "2.12.3"
lazy val akkaVersion = "2.6.14"
lazy val akkaHttpVersion = "10.2.3"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.3" % Test,

  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.apache.commons" % "commons-lang3" % "3.12.0",

  "com.typesafe.akka" %% "akka-stream" % akkaVersion,

  "io.vertx" % "vertx-json-schema" % "4.0.3",

  "services.scalable" %% "index" % "0.1"
)

dependencyOverrides += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.1"

enablePlugins(AkkaGrpcPlugin)