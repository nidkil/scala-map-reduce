package com.nidkil.mapreduce.actors

import akka.actor.{ Actor }
import scala.collection.mutable.Map
import scala.collection.mutable.StringBuilder
import akka.actor.ActorRef
import akka.actor.ActorLogging

object CountAggregator {
  case class Aggregate(localWordCntMap: Map[String, Int])
  case object GetWordCnt
  case object PrintWordCntMap
}

class CountAggregator(status: ActorRef) extends Actor with ActorLogging {

  import CountAggregator._
  import Status._
  
  val wordCntMap = Map[String, Int]()
  var count: Integer = 0;

  def receive = {
    case aggregate: Aggregate => {
      log.info("Received Aggregate")      
      aggregate.localWordCntMap map (x => wordCntMap += ((x._1, wordCntMap.getOrElse(x._1, 0) + x._2)))
      status ! ChunkCompleted
    }
    case PrintWordCntMap => {
      log.info("Received PrintWordCntMap")      
      println(getResult)
    }
    case GetWordCnt => {
      log.info("Received GetWordCnt")      
      sender ! getResult
    }
    case x => log.warning(s"Unknown message received by ${self.path} [${x.getClass}, value=$x]")
  }

  def getResult(): String = {
    val sb = new StringBuilder() 
    var cnt = 0;
    for (word <- wordCntMap.keys) {
      if (cnt > 0) sb.append(", ") 
      if (cnt % 10 == 0) sb.append("\n")
      sb.append(s"$word => ${wordCntMap.get(word).get}")
      cnt += 1
    }
    sb.append("\n")
    sb.toString()
  }

}