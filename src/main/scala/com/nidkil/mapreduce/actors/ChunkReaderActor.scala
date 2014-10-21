package com.nidkil.mapreduce.actors

import com.nidkil.utils.WordBoundryChunkReader
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import com.nidkil.splitter.Chunk
import akka.actor.ActorLogging

object ChunkReaderActor {
  case class ProcessChunk(chunk: Chunk)
}

class ChunkReaderActor(sanatizeAndSplit: ActorRef) extends Actor with ActorLogging {

  import ChunkReaderActor._
  import SanatizeSplitGroupActor._

  def receive = {
    case processChunk: ProcessChunk => {
      log.info(s"Received ProcessChunk [$processChunk]")      
      val reader = new WordBoundryChunkReader(processChunk.chunk)

      sanatizeAndSplit ! new Sanatize(reader.readChunk())
    }
    case x => log.warning(s"Unknown message received by ${self.path} [${x.getClass}, value=$x]")
  }

}
