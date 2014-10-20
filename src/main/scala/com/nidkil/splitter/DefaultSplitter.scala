package com.nidkil.splitter

import com.nidkil.mapreduce.Chunk

import java.io.File
import java.io.IOException

import scala.collection.mutable.LinkedHashSet

object DefaultSplitter {
  
  // 5 MB
  private val defaultMaxChunkSize = 1024 * 5  

}

class DefaultSplitter extends Splitter {

  import DefaultSplitter._
  
  private var _maxChunkSize = defaultMaxChunkSize  
    
  def maxChunkSize = _maxChunkSize
  
  def maxChunkSize(value: Int) : Unit = _maxChunkSize = value
  
  def split(file : File) : LinkedHashSet[Chunk] = {
    if (!file.exists() || !file.isFile()) {
      throw new IOException(s"File does not exist or is not a file [${file.getAbsolutePath()}]")
    }
    
    var chunks = LinkedHashSet[Chunk]() 
    var numOfChunks = (file.length / maxChunkSize).toInt
    val restChunkSize = file.length % maxChunkSize

    if (restChunkSize > 0) numOfChunks += 1

    for (i <- 1 to numOfChunks) {
      val startChunk = (i - 1) * maxChunkSize
      val endChunk = maxChunkSize + startChunk

      // Make sure we get the whole file and do not exceed the file size
      if (i == numOfChunks) {
        chunks += new Chunk(i, file.getAbsolutePath(), startChunk, file.length, (file.length - startChunk).toInt)
      } else {
        chunks += new Chunk(i, file.getAbsolutePath(), startChunk, endChunk, maxChunkSize.toInt)
      }
    }
    
    chunks
  }

}