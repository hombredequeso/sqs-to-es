import Dependencies._

ThisBuild / scalaVersion     := "2.13.1"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.

scalaVersion := "2.13.1"

name := "akpakka-streams"
organization := "hdq"
version := "1.0"

libraryDependencies += "org.typelevel" %% "cats-core" % "2.0.0"

libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.25"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "1.1.1"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-elasticsearch" % "1.1.1"

libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-sqs" % "1.1.1"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.26"
 
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.5.25" % Test
libraryDependencies +=  "org.scalatest" %% "scalatest" % "3.0.8" % Test
libraryDependencies += "org.mockito" % "mockito-core" % "3.1.0" % Test
