package com.nidkil.utils

import java.io.{ Closeable, File, IOException, RandomAccessFile }
import java.nio.{ ByteBuffer, CharBuffer, MappedByteBuffer }
import java.nio.channels.FileChannel
import java.nio.charset.Charset
import com.nidkil.splitter.Chunk

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
class WordBoundryChunkReader(chunk: Chunk) {

  val file = new File(chunk.filePath)

  if (!file.exists() || !file.isFile()) {
    throw new IOException(s"File does not exist or is not a file [${file.getPath}]")
  }

  var channel: FileChannel = new RandomAccessFile(file, "r").getChannel()

  def readChunk() = {
    try {
      var readChunkSize: Long = chunk.size * 2

      // Make sure we do not read past the end of the file
      if (chunk.start + readChunkSize > file.length) readChunkSize = file.length - chunk.start

      val memoryMappedFile = channel.map(FileChannel.MapMode.READ_ONLY, chunk.start, readChunkSize)

      // Find the end of the first word in the buffer, starting from the beginning of the buffer
      val startPos = if (chunk.start == 0) 0 else findEndOfWordBoundry(memoryMappedFile, 0, chunk.size)

      // Find the end of the last word in the buffer, starting from the start of the buffer second chunk
      val endPos = if (chunk.size >= readChunkSize) readChunkSize.toInt else findEndOfWordBoundry(memoryMappedFile, chunk.size, readChunkSize)

      memoryMappedFile.position(startPos)
      memoryMappedFile.limit(endPos)

      Charset.defaultCharset().newDecoder().decode(memoryMappedFile.load).toString()
    } finally {
      channel.close()
    }
  }

  private def findEndOfWordBoundry(buffer: MappedByteBuffer, start: Long, end: Long): Int = {
    if(buffer.get(start.toInt).asInstanceOf[Char].equals(' ') || start + 1 == end) start.toInt else findEndOfWordBoundry(buffer, start + 1, end)    
  }

}