package com.nidkil.mapreduce.actors

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import akka.actor.ActorLogging

object SanatizeSplitGroupActor {
  case class Sanatize(text: String)
}

class SanatizeSplitGroupActor(localAgg: ActorRef) extends Actor with ActorLogging {

  import LocalAggregatorActor._
  import SanatizeSplitGroupActor._
  
  def receive = {
    case sanatize: Sanatize => {
      log.info("Received Sanatize")      
      // Remove punctuation and other special characters
      val punctuation = """[\.:;!?,|\"\(\)\[\]&%-`^#=]"""
      // Replace EOL with space and then remove punctuation
      val result = sanatize.text.replaceAll("(\\r|\\n)", " ").replaceAll(punctuation, "")
      // Split and group
      localAgg ! new AggregateCountMap(result.split(" ").groupBy(x => x))
    }
    case x => log.warning(s"Unknown message received by ${self.path} [${x.getClass}, value=$x]")
  }

}
