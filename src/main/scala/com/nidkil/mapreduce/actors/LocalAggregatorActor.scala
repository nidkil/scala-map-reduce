package com.nidkil.mapreduce.actors

import akka.actor.{ Actor, ActorRef }
import akka.actor.ActorLogging

object LocalAggregatorActor {
  case class AggregateCountMap(countMap: Map[String, Array[String]])
}

class LocalAggregatorActor(globalAgg: ActorRef) extends Actor with ActorLogging {

  import LocalAggregatorActor._
  import CountAggregatorActor._

  val wordCountMap = scala.collection.mutable.Map[String, Int]()
  
  def receive = {
    case aggregateCountMap: AggregateCountMap => {
      log.info("Received AggregateCountMap")      
      aggregateCountMap.countMap map {
        case (k, v) =>
          wordCountMap += ((k, wordCountMap.getOrElse(k, 0) + v.size))
      }
      globalAgg ! new Aggregate(wordCountMap)
    }
    case x => log.warning(s"Unknown message received by ${self.path} [${x.getClass}, value=$x]")
  }

}