package nl.newparadigm.downloader

import akka.actor.{ Actor, ActorSystem, Props }
import akka.routing.{ Broadcast, FromConfig, RoundRobinRouter }
import com.typesafe.config.ConfigFactory
import java.io.File

case class Chunk(id: Int, filePath : String, start : Long, end: Long)

class ChunkGeneratorActor extends Actor {

  def receive = {
    case filePath: String =>
      // Determine the number of chunks
      val file = new File(filePath)
      val chunkSize = 1000
      var numChunks = (file.length / chunkSize).toInt
      val restChunkSize = file.length % chunkSize

      if(restChunkSize > 0) numChunks += 1

      val system = ActorSystem("System", ConfigFactory.load("application.conf"))
      val stats = system.actorOf(Props(new StatsActor(numChunks)), "stats")
      val chunkDownloaderRouted = system.actorOf(Props(new ChunkDownloaderActor(stats)).withRouter(FromConfig()), "chunkDownloaderRouted")

      println(s"stats=${stats.path.toString} [${stats.path.name}]")
      
      stats ! "start"
      
      for (id <- 1 to numChunks) {
        val startChunk = (id - 1) * chunkSize 
        val endChunk = chunkSize + startChunk
        
        // Make sure that when get the while file and do not exceed the file size
        if(id == numChunks) {
          println(s" - Chunk ${id} of ${numChunks} FINAL")
          
          chunkDownloaderRouted ! new Chunk(id, filePath, file.length - restChunkSize, file.length)
        } else {
          println(s" - Chunk ${id} of ${numChunks}")
          
          chunkDownloaderRouted ! new Chunk(id, filePath, startChunk, endChunk)
        }
      }

      // Broadcast end of day message
      chunkDownloaderRouted ! Broadcast(true)

      println(s"Chunks generated [chunkSize=${chunkSize}, numOfChunks=${numChunks}, restChunkSize=${restChunkSize}, fileSize=${file.length}]")
  }

}