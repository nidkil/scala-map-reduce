package com.nidkil.splitter

import com.nidkil.mapreduce.Chunk

import java.io.File

import scala.collection.mutable.LinkedHashSet

trait Splitter {

  def maxChunkSize() : Long
  
  def split(file : File) : LinkedHashSet[Chunk]  
  
}