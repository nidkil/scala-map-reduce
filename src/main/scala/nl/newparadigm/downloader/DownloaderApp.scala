package nl.newparadigm.downloader

import akka.actor.{ ActorSystem, Props }
import akka.actor.actorRef2Scala
import com.typesafe.config.ConfigFactory

object DownloaderApp extends App {

  def getCurDir = new java.io.File(".").getCanonicalPath

  override def main(args: Array[String]) {
    val system = ActorSystem("System", ConfigFactory.load("application.conf"))
    val chunkGenerator = system.actorOf(Props[ChunkGeneratorActor], "chunkGenerator")
    
    chunkGenerator ! getCurDir + "/src/main/scala/resources/othello.txt"
  }

}