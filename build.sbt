import sbt._
import Process._
import Keys._
import AssemblyKeys._

name := "spear"

version := "1.0.0"

scalaVersion := "2.10.4"

resolvers += DefaultMavenRepository

libraryDependencies ++= List(
  ("org.apache.spark" % "spark-core_2.10" % "1.3.1") % "provided",
  "org.mongodb" %% "casbah" % "2.8.1",
  "com.foursquare" % "common-thrift-bson" % "3.0.0-M11",
  "com.foursquare" %% "rogue-field"         % "2.5.0" intransitive(),
  "com.foursquare" %% "rogue-core"          % "3.0.0-beta13.1" intransitive(),
  "com.foursquare" %% "rogue-lift"          % "3.0.0-beta13.1" intransitive(),
  "com.foursquare" %% "rogue-index"         % "3.0.0-beta13.1" intransitive(),
  "com.foursquare" %% "rogue-spindle"          % "3.0.0-beta13.1" intransitive(),
  "net.liftweb"    %% "lift-mongodb-record" % "2.6"
)

seq(thriftSettings: _*)

assemblySettings

test in assembly := {}

jarName in assembly := "spear-with-dependencies-1.0.0-SNAPSHOT.jar"
