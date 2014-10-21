package com.nidkil.mapreduce.actors

import akka.actor.{ Actor }
import scala.collection.mutable.Map
import akka.actor.ActorRef
import com.nidkil.splitter.Chunk
import akka.actor.ActorLogging

object StatusActor {
  case class ChunkCompleted()
}

class StatusActor(test: ActorRef, chunkCount: Int) extends Actor with ActorLogging {

  import ChunkGeneratorActor._
  import StatusActor._
  
  lazy val originalSender = context.sender
  var chunksCompleted = 0;

  def receive = {
    case chunkCompleted: ChunkCompleted => {
      log.info(s"Received ThreadCompleted [chunkCount=$chunkCount, chunksCompleted=${chunksCompleted + 1}, test=${test.path}, sender=${sender.path.parent}, originalSender=${originalSender.path}]")
      
      chunksCompleted += 1

      //TODO How to send response to sender
      if (chunkCount == chunksCompleted) test ! new Completed()
    }
    case x => log.warning(s"Unknown message received by ${self.path} [${x.getClass}, value=$x]")
  }

}