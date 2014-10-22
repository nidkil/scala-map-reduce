package com.nidkil.mapreduce.actors

import com.nidkil.utils.Timer
import akka.actor.Actor
import akka.actor.ActorLogging

object Stats {
  case object StartTimer
  case object StopTimer
  case object GetExecTime
}

class Stats extends Actor with ActorLogging {

  import Stats._

  private lazy val timer = new Timer()

  def receive = {
    case StartTimer => {
      log.info("Received startTimer")      
      timer.start()
      sender ! "started"
    }
    case StopTimer => {
      log.info("Received stopTimer")      
      timer.stop()
      sender ! "stopped"
    }
    case GetExecTime => {
      log.info("Received printExecTime")      
      sender ! timer.execTime(false)
    }
    case x => log.warning(s" +++ Unknown message received by ${self.path} [${x.getClass}, value=$x]")
  }

}