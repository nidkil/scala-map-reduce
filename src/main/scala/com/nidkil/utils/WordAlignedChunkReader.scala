package com.nidkil.utils

import java.io.{ Closeable, File, IOException, RandomAccessFile }
import java.nio.{ ByteBuffer, CharBuffer, MappedByteBuffer }
import java.nio.channels.FileChannel
import java.nio.charset.Charset

/**
 * This implementation uses memory mapped files. Memory mapped files are special
 * files in Java which allows Java program to access contents  directly from
 * memory, this is achieved by mapping whole file or portion of file into memory
 * and operating system takes care of loading page requested and writing into
 * file while application only deals with memory which results in very fast IO
 * operations. Memory used to load Memory mapped file is outside of Java heap
 * Space. Java programming language supports memory mapped file with java.nio
 * package and has MappedByteBuffer to read and write from memory.
 *
 * Read more: http://javarevisited.blogspot.com/2012/01/memorymapped-file-and-io-in-java.html#ixzz3GOKdyqDS
 */
class WordAlignedChunkReader(filePath: String, chunkId: Int, chunkStart: Long, chunkEnd: Long, chunkSize: Int) {

  val file = new File(filePath)

  if (!file.exists() || !file.isFile()) {
    throw new IOException(s"File does not exist or is not a file [${file.getPath}]")
  }

  var channel: FileChannel = new RandomAccessFile(file, "r").getChannel()

  def readChunk() = {
    var readChunkSize: Long = chunkSize * 2

    // Make sure we do not read past the end of the file
    if (chunkStart + readChunkSize > file.length) readChunkSize = file.length - chunkStart

    val memoryMappedFile = channel.map(FileChannel.MapMode.READ_ONLY, chunkStart, readChunkSize)

    // Find the end of the first word in the buffer, starting from the beginning of the buffer
    val startPos = if (chunkStart == 0) 0 else findEndOfWordBoundry(memoryMappedFile, 0, chunkSize)

    // Find the end of the last word in the buffer, starting from the start of the buffer second chunk
    val endPos = if(chunkSize > readChunkSize) readChunkSize.toInt else findEndOfWordBoundry(memoryMappedFile, chunkSize, readChunkSize)

    memoryMappedFile.position(startPos)
    memoryMappedFile.limit(endPos)

    Charset.defaultCharset().newDecoder().decode(memoryMappedFile.load).toString()
  }
  
  private def findEndOfWordBoundry(buffer: MappedByteBuffer, start: Long, end: Long): Int = {
    var c = buffer.get(start.toInt).asInstanceOf[Char]
    var pos = start

    // Keep moving forward until a space is found (space marks end of word boundry)
    if (pos == end)
      pos.toInt
    else {
      while (!c.equals(' ') && pos < end) {
        pos += 1
        c = buffer.get(pos.toInt).asInstanceOf[Char]
      }      
      pos.toInt
    }
  }

}