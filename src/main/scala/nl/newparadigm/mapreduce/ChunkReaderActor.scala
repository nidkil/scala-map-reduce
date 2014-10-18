package nl.newparadigm.mapreduce

import akka.actor.{ ActorRef, Actor, ActorSystem, Props }
import java.io.{ File, IOException, RandomAccessFile }
import java.nio.channels.FileChannel

class ChunkReaderActor(localAgg: ActorRef) extends Actor {

  def receive = {
    case chunk: Chunk => {
      val reader = new WordAlignedChunkReader(chunk.filePath, chunk.id, chunk.start, chunk.end, chunk.size)

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
