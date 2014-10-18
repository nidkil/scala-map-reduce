package nl.newparadigm.mapreduce

import akka.actor.{ Actor, ActorSystem, PoisonPill, Props }
import akka.routing.{ Broadcast, RoundRobinRouter }

import java.io.{ File, IOException, RandomAccessFile }
import java.nio.channels.FileChannel

case class Chunk(id: Int, filePath : String, start : Long, end: Long, size: Int)

class ChunkGeneratorActor extends Actor {

  def receive = {
    case filePath: String =>
      val file = new File(filePath)
      
      if(!file.exists() || !file.isFile()) {
        throw new IOException(s"File does not exist or is not a file [$filePath]")
      }

      val system = ActorSystem("System")
      val stats = system.actorOf(Props(new StatsActor), "stats")
      
      stats ! "start"
      
      val countAggr = system.actorOf(Props(new CountAggregator(8)))
      val localAggr = system.actorOf(Props(new LocalAggregator(countAggr)))
      val router = system.actorOf(Props(new ChunkReaderActor(localAggr)).withRouter(RoundRobinRouter(nrOfInstances = 8)))

      // Determine the number of chunks
      val chunkSize = 50000
      var numOfChunks = (file.length / chunkSize).toInt
      val restChunkSize = file.length % chunkSize

      if(restChunkSize > 0) numOfChunks += 1
      
      for (id <- 1 to numOfChunks) {
        val startChunk = (id - 1) * chunkSize 
        val endChunk = chunkSize + startChunk
        
        // Make sure we get the whole file and do not exceed the file size
        if(id == numOfChunks) {
          println(s" - Chunk ${id} of ${numOfChunks} FINAL")
          router ! new Chunk(id, filePath, file.length - restChunkSize, file.length, chunkSize)
        } else {
          println(s" - Chunk ${id} of ${numOfChunks}")
          router ! new Chunk(id, filePath, startChunk, endChunk, chunkSize) 
        }
      }

      // Use Broadcast to send a PoisonPill to all Actors controlled by the 
      // router. If the PoisonPill is sent directly to a router it will
      // interperate it as its own command. Broadcast ensures the message
      // is unwrapped from Broadcast by the router and then forwarded to 
      // every actor it is routing for.
      //router ! Broadcast(PoisonPill)
      router ! Broadcast(true)

      println(s"Chunks generated [chunkSize=${chunkSize}, numOfChunks=${numOfChunks}, restChunkSize=${restChunkSize}, fileSize=${file.length}]")
  }

}