package nl.newparadigm.downloader

import akka.actor.{ Actor, ActorSystem, Props }
import nl.newparadigm.utils.Timer

case class StatsInit(numOfChunks: Int)
case class StatsDownload(chunk: Chunk, status: String)

class StatsActor(numOfChuncks: Int) extends Actor {

    private lazy val timer = new Timer()
    private var completed = 0
        
    println(s" ---> Stats initialized: numOfChuncks=$numOfChuncks")
    
    def receive = {
      case "start" => timer.start()
      case "stop" => {
        timer.stop()
        
        println(s"\nExecution time ==> ${timer.execTime(true)}")
      }
      case "stats" => println("")
      case "completed" => {
        completed += 1
        if(completed == numOfChuncks) {
          val system = ActorSystem("System")
          val chunCombiner = system.actorOf(Props(new ChunkCombinerActor))
          chunCombiner ! "combine"
        }
        
        println(s" ---> Stats complete: numOfChuncks=$numOfChuncks, completed=$completed")
      }
      case statsDownload: StatsDownload => {
        println(statsDownload)
      }
      case _ => println("\nUnknown message received")
    }
    
}