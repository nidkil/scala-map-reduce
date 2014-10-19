package com.nidkil.mapreduce

import com.nidkil.utils.Timer

import akka.actor.Actor

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