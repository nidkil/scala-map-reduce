name := "ScalaMapReduce"

organization := "New Paradigm"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.2"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
 
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.6"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test"