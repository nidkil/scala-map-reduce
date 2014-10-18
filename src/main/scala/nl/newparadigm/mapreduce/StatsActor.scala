package nl.newparadigm.mapreduce

import akka.actor.{ Actor, ActorSystem, Props }
import nl.newparadigm.utils.Timer

class StatsActor extends Actor {

    private lazy val timer = new Timer()
    
    def receive = {
      case "start" => {
        println("Starting timer")
        timer.start()
      }
      case "stop" => {
        println("Stopping timer")
        timer.stop()
        
        println(s"\nExecution time ==> ${timer.execTime(false)}")
      }     
      case _ => println("\nUnknown message received")
    }
    
}