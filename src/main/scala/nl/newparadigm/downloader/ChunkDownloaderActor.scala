package nl.newparadigm.downloader

import akka.actor.{ Actor, ActorRef }

class ChunkDownloaderActor(stats : ActorRef) extends Actor {

  val random = new scala.util.Random

  def receive = {
    case chunk: Chunk => {
      Thread.sleep(random.nextInt(500))
      
      println(s"path=${self.path.name}, parent=${self.path.parent.name}, root=${self.path.root.name}: ${chunk}")
      
      stats ! "completed"
    }
  }

}