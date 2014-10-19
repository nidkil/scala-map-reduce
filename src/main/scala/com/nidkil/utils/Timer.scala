package com.nidkil.utils

class Timer {

  private var startTime = 0L
  private var stopTime = 0L
  
  def start() = startTime = System.currentTimeMillis
  
  def stop() = stopTime = System.currentTimeMillis
  
  def execTime(msecs : Boolean = true) : String = {
    val diff = stopTime - startTime
    if(msecs) s"$diff msecs" 
    else "%s,%s secs".format(diff / 1000, (diff % 1000).toString().padTo(4, "0").mkString) 
  }

}