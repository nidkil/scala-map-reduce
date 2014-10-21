package com.nidkil.mapreduce.actors

import com.nidkil.utils.Timer
import akka.actor.Actor
import akka.actor.ActorLogging

object Stats {
  case class StartTimer()  
  case class StopTimer()
  case class PrintExecTime()
}

class Stats extends Actor with ActorLogging {

  import Stats._

  private lazy val timer = new Timer()

  def receive = {
    case startTimer : StartTimer => {
      log.info("Received startTimer")      
      timer.start()
    }
    case stopTimer: StopTimer => {
      log.info("Received stopTimer")      
      timer.stop()
    }
    case printExecTime: PrintExecTime => {
      log.info("Received printExecTime")      
      println(s"Execution time ==> ${timer.execTime(false)}")
    }
    case x => log.warning(s" +++ Unknown message received by ${self.path} [${x.getClass}, value=$x]")
  }

}