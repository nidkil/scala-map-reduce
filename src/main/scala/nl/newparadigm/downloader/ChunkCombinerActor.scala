package nl.newparadigm.downloader

import akka.actor.{ Actor }

class ChunkCombinerActor extends Actor {

  def receive = {
    case combine => {
      println("Combining")
    }
  }

}