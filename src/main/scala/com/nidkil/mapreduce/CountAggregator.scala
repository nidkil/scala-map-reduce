package com.nidkil.mapreduce

import akka.actor.{ Actor }

class CountAggregator(threadCount: Int) extends Actor {

  val wordCntMap = scala.collection.mutable.Map[String, Int]()
  var count: Integer = 0;

  def receive = {
    case localCount: scala.collection.mutable.Map[String, Int] => {
      localCount map (x => wordCntMap += ((x._1, wordCntMap.getOrElse(x._1, 0) + x._2)))

      count = count + 1

      if (count == threadCount) {
        onCompletion
      }
    }
  }

  def onCompletion() {

    println("All messages received...")

    var cnt = 0;
    
    for (word <- wordCntMap.keys) {
      if(cnt > 0) print(", ")
      if(cnt % 10 == 0) println()
      print(s"$word => ${wordCntMap.get(word).get}") 
      cnt += 1
    }

    println("\n")
    
    context.actorSelection("/user/stats") ! "stop"
  }

}