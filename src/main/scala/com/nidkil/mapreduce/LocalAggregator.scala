package com.nidkil.mapreduce

import akka.actor.{ Actor, ActorRef }

class LocalAggregator(globalAgg: ActorRef) extends Actor {

  val wordCountMap = scala.collection.mutable.Map[String, Int]()
  
  def receive = {
    case countMap: Map[String, Array[String]] => {
      countMap map {
        case (k, v) =>
          wordCountMap += ((k, wordCountMap.getOrElse(k, 0) + v.size))
      }
      
      globalAgg ! wordCountMap
    }
    case complete: Boolean => globalAgg ! wordCountMap
  }

}