package com.nidkil.mapreduce

import com.nidkil.utils.WordBoundryChunkReader

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.actorRef2Scala

class ChunkReaderActor(localAgg: ActorRef) extends Actor {

  def receive = {
    case chunk: Chunk => {
      val reader = new WordBoundryChunkReader(chunk)

      //TODO Remove "leestekens", EOL, etc.
      // Send chunk
      val read = reader.readChunk().replaceAll("\n", " ")

      println(s" ----> chunkId=${chunk.id}: $read")
          
      localAgg ! read.split(" ").groupBy(x => x)

      println(s" -- Chunk #${chunk.id} completed [start=${chunk.start}, end=${chunk.end}]")
    }
    case (done: Boolean) => localAgg ! done
  }

}
