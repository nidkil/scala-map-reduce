package com.nidkil.mapreduce.actors

import java.io.File
import java.io.IOException
import com.nidkil.splitter.DefaultSplitter
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection.toScala
import akka.pattern.ask
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.routing.Broadcast
import akka.routing.RoundRobinRouter
import akka.actor.ActorSystem
import akka.actor.ActorLogging
import akka.actor.Terminated
import scala.concurrent.Await
import akka.util.Timeout
import scala.concurrent.duration._

object Controller {
  case class Start(filePath: String)
  case object Completed
}

class Controller extends Actor with ActorLogging {

  import CountAggregator._
  import Controller._
  import ChunkReader._
  import Stats._
  import Reaper._

  var numChunks = 0
  var countAggr, localAggr, router, sanatizeSplitGroup, shutdownReaper, stats, status: ActorRef = context.system.deadLetters

  def allSoulsReaped() {
      log.info(s"All souls reaped, shutting system down")
      context.stop(context.self)
      context.system.shutdown          
  }

  def receive = {
    case start: Start => {
      log.info(s"Received start [filePath=${start.filePath}]")

      val file = new File(start.filePath)

      if (!file.exists() || !file.isFile()) {
        throw new IOException(s"File does not exist or is not a file [${start.filePath}]")
      }

      stats = context.actorOf(Props(new Stats), "stats")

      stats ! StartTimer

      val splitter = new DefaultSplitter()
      val chunks = splitter.split(file)

      numChunks = chunks.size

      status = context.actorOf(Props(new Status(context.self, numChunks)), "status")
      countAggr = context.actorOf(Props(new CountAggregator(status)), "countAggregator")
      localAggr = context.actorOf(Props(new LocalAggregator(countAggr)), "localAggregator")
      sanatizeSplitGroup = context.actorOf(Props(new SanatizeSplitGroup(localAggr)), "sanatizeSplitGroup")
      router = context.actorOf(Props(new ChunkReader(sanatizeSplitGroup)).withRouter(RoundRobinRouter(nrOfInstances = 8)), "chunkReader")
      shutdownReaper =  context.actorOf(Props(new ShutdownReaper(context.self)), "shutdownReaper")

      shutdownReaper ! WatchMe(countAggr)
      shutdownReaper ! WatchMe(localAggr)
      shutdownReaper ! WatchMe(router)
      shutdownReaper ! WatchMe(sanatizeSplitGroup)
      shutdownReaper ! WatchMe(stats)
      shutdownReaper ! WatchMe(status)

      chunks.foreach(router ! new ProcessChunk(_))

      log.info(s"Chunks generated [maxChunkSize=${splitter.maxChunkSize()} KB, numOfChunks=${chunks.size}, fileSize=${file.length}]")
    }
    case Completed => {
      log.info("Received Completed")

      implicit val timeout = Timeout(5.seconds)

      val wordCnt = Await.result(countAggr ? GetWordCnt, timeout.duration).asInstanceOf[String]
      println(wordCnt)

      Await.result(stats ? StopTimer, timeout.duration)

      val execTime = Await.result(stats ? GetExecTime, timeout.duration).asInstanceOf[String]

      println(s"\nexecTime=$execTime\n")

      countAggr ! PoisonPill
      localAggr ! PoisonPill
      router ! Broadcast(PoisonPill)
      sanatizeSplitGroup ! PoisonPill
      stats ! PoisonPill
      status ! PoisonPill
    }
    case x => log.warning(s" Unknown message received by ${self.path} [${x.getClass}, value=$x]")
  }

}