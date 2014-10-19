package nl.newparadigm.mapreduce

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
    var readChunkSize: Int = chunkSize * 2

    // Make sure we do not read past the end of the file
    val lastChunk = if (chunkStart + readChunkSize > file.length) {
      readChunkSize = (file.length - chunkStart).toInt
      true
    } else false

    val memoryMappedFile = channel.map(FileChannel.MapMode.READ_ONLY, chunkStart, readChunkSize)

    // Find the end of the first word in the buffer, starting from the beginning of the buffer
    val startPos = if (chunkStart == 0) 0 else findEndOfWordBoundry(memoryMappedFile, 0, chunkSize)

      println(s"chunkSize=$chunkSize, readChunkSize=$readChunkSize")
    // Find the end of the last word in the buffer, starting from the start of the buffer second chunk
    val endPos = if(lastChunk) readChunkSize.toInt else findEndOfWordBoundry(memoryMappedFile, chunkSize, readChunkSize)

      println(s"startPos=$startPos, endPos=$endPos")
    if(startPos == -1 || endPos == -1) {
      ""
    } else {
      memoryMappedFile.position(startPos)
      memoryMappedFile.limit(endPos)
  
      Charset.defaultCharset().newDecoder().decode(memoryMappedFile.load).toString()      
    }
  }
  
  private def findEndOfWordBoundry(buffer: MappedByteBuffer, start: Int, end: Int): Int = {
    var pos = start
    var c = buffer.get(pos).asInstanceOf[Char]
    
      println(s"start=$start, end=$end, buffer.capacity=${buffer.capacity}")
    // Keep moving forward until a space is found (space marks end of word boundry)
    for(i <- start to end if !c.equals(' ') if i < buffer.capacity) {
      println(s"c=$c [i=$i] [pos=$pos]")
      c = buffer.get(i).asInstanceOf[Char]
      pos = i
    }
    
      println(s"pos=$pos, buffer.capacity-1=${buffer.capacity - 1}")
    // If we reach the end of the buffer, then we are processing the last chunk. The
    // word has been processed as part of the previous pass, so it can be ignored.
    if(pos == buffer.capacity() - 1) -1 else pos
  }

}