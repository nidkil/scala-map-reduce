package com.nidkil.mapreduce.actors

import java.io.File
import java.io.IOException
import com.nidkil.splitter.DefaultSplitter
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection.toScala
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.routing.Broadcast
import akka.routing.RoundRobinRouter
import akka.actor.ActorSystem
import akka.actor.ActorLogging

object ChunkGeneratorActor {
  case class Start(filePath: String)
  case class Completed()
}

class ChunkGeneratorActor extends Actor with ActorLogging {

  import CountAggregatorActor._
  import ChunkGeneratorActor._
  import ChunkReaderActor._
  import StatsActor._

  var numChunks = 0
  var router, stats, countAggr: ActorRef = null

  def receive = {
    case start: Start => {
      log.info(s"Received start [filePath=${start.filePath}]")

      val file = new File(start.filePath)

      if (!file.exists() || !file.isFile()) {
        throw new IOException(s"File does not exist or is not a file [${start.filePath}]")
      }

      stats = context.actorOf(Props(new StatsActor), "stats")

      stats ! StartTimer()

      val splitter = new DefaultSplitter()
      val chunks = splitter.split(file)

      numChunks = chunks.size

      val status = context.actorOf(Props(new StatusActor(context.self, numChunks)), "status")
      countAggr = context.actorOf(Props(new CountAggregatorActor(status)), "countAggregator")
      val localAggr = context.actorOf(Props(new LocalAggregatorActor(countAggr)), "localAggregator")
      val sanatizeSplitGroup = context.actorOf(Props(new SanatizeSplitGroupActor(localAggr)), "sanatizeSplitGroup")
      router = context.actorOf(Props(new ChunkReaderActor(sanatizeSplitGroup)).withRouter(RoundRobinRouter(nrOfInstances = 8)), "chunkReader")

      chunks.foreach(router ! new ProcessChunk(_))

      log.info(s"Chunks generated [maxChunkSize=${splitter.maxChunkSize()} KB, numOfChunks=${chunks.size}, fileSize=${file.length}]")
    }
    case completed: Completed => {
      log.info("Received Completed")

      countAggr ! new PrintWordCntMap()
      
      //TODO replave with future
      // Intentionally sleep to give time to print word count
      Thread.sleep(2000)
      
      stats ! new StopTimer()
      stats ! new PrintExecTime()

      router ! Broadcast(PoisonPill)
      self ! PoisonPill

      //TODO replave with future
      // Intentionally sleep to give actors time to shutdown
      Thread.sleep(1000)
      
      context.system.shutdown
    }
    case x => log.warning(s" **** Unknown message received by ${self.path} [${x.getClass}, value=$x]")
  }

}