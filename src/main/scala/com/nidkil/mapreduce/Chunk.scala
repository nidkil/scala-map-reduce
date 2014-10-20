package com.nidkil.mapreduce

case class Chunk(id: Int, filePath : String, start : Long, end: Long, size: Int)