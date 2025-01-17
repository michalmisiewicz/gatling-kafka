name := "gatling-kafka"

organization := "com.github.mnogu"

version := "0.2.0-SNAPSHOT"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "io.gatling" % "gatling-core" % "3.1.2" % "provided",
  ("org.apache.kafka" % "kafka-clients" % "1.1.0")
    // Gatling contains slf4j-api
    .exclude("org.slf4j", "slf4j-api")
)

// Gatling contains scala-library
assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false)
