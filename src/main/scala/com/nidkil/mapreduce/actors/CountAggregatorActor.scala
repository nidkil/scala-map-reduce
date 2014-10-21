package com.nidkil.mapreduce.actors

import akka.actor.{ Actor }
import scala.collection.mutable.Map
import akka.actor.ActorRef
import akka.actor.ActorLogging

object CountAggregatorActor {
  case class Aggregate(localWordCntMap: Map[String, Int])
  case class PrintWordCntMap()
}

class CountAggregatorActor(status: ActorRef) extends Actor with ActorLogging {

  import CountAggregatorActor._
  import StatusActor._
  
  val wordCntMap = Map[String, Int]()
  var count: Integer = 0;

  def receive = {
    case aggregate: Aggregate => {
      log.info("Received Aggregate")      
      aggregate.localWordCntMap map (x => wordCntMap += ((x._1, wordCntMap.getOrElse(x._1, 0) + x._2)))
      status ! new ChunkCompleted()
    }
    case printWordCntMap : PrintWordCntMap => {
      log.info("Received PrintWordCntMap")      
      printResult
    }
    case x => log.warning(s"Unknown message received by ${self.path} [${x.getClass}, value=$x]")
  }

  def printResult() {
    var cnt = 0;
    for (word <- wordCntMap.keys) {
      if (cnt > 0) print(", ")
      if (cnt % 10 == 0) println()
      print(s"$word => ${wordCntMap.get(word).get}")
      cnt += 1
    }
    println("\n")
  }

}