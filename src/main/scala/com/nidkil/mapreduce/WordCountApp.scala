package com.nidkil.mapreduce

import akka.actor.{ ActorSystem, Props }
import com.typesafe.config.ConfigFactory

object WordCountApp extends App {

  def getCurDir = new java.io.File(".").getCanonicalPath

  override def main(args: Array[String]) {
    val system = ActorSystem("System", ConfigFactory.load("application.conf"))
    val chunkGenerator = system.actorOf(Props[ChunkGeneratorActor], "chunkGenerator")

    chunkGenerator ! getCurDir + "/src/main/scala/resources/othello.txt"
  }

}