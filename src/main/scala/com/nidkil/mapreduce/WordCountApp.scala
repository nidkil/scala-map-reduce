package com.nidkil.mapreduce

import akka.actor.{ ActorSystem, Props }
import com.typesafe.config.ConfigFactory
import com.nidkil.mapreduce.actors.Controller

object WordCountApp extends App {

  import Controller._
  
  def getCurDir = new java.io.File(".").getCanonicalPath

  override def main(args: Array[String]) {
    val system = ActorSystem("System", ConfigFactory.load("application.conf"))
    val controller = system.actorOf(Props[Controller], "controller")

    controller ! Start(getCurDir + "/src/main/scala/resources/othello.txt")
  }

}