package nl.newparadigm.mapreduce

import java.io.{ File, FileWriter, IOException, PrintWriter }

object Test extends App {

  def writeToFile(filePath: String, data: String): Unit = {
    val pw = new java.io.PrintWriter(new File(filePath))
    try pw.write(data) finally pw.close()
  }

  def printToFile(f: File)(data: String) {
    println(s"#1 $f")
    val fileWriter = new FileWriter(f)
    try {
    println(s"#2 $data")
      fileWriter.write(data)
    } finally {
      fileWriter.close()
    }
  }

  override def main(args: Array[String]) {
  val testData = """This is a test sentence to test the WordAlignedChunkReader and see if it cuts 
of the lines in the correct position. We also need to see if the whole file is 
processed. A very exiting test. At least I think it is as this has been bugging 
me for a while."""

    val filePath = "test.dat"
    val file = new File(filePath)

    if (!file.exists() || !file.isFile()) {
      throw new IOException(s"File does not exist or is not a file [$filePath]")
    }
    
    //printToFile(new File(filePath)) { testData }
    println(s"testData=$testData")
    writeToFile(filePath, testData)

   // println(s"File ${file.getAbsolutePath}")

    val chunkSize = 60
    var numOfChunks = (file.length / chunkSize).toInt
    val restChunkSize = file.length % chunkSize

    if (restChunkSize > 0) numOfChunks += 1

    for (id <- 1 to numOfChunks) {
      val startChunk = (id - 1) * chunkSize
      val endChunk = chunkSize + startChunk

      // Make sure we get the whole file and do not exceed the file size
      if (id == numOfChunks) {
        //println(s" - Chunk ${id} of ${numOfChunks} FINAL")
        val reader = new WordAlignedChunkReader(filePath, id, file.length - restChunkSize, file.length, chunkSize)
        val read = reader.readChunk().replaceAll("\n", " ")
        println(s"$id=$read")
      } else {
       // println(s" - Chunk ${id} of ${numOfChunks}")
        val reader = new WordAlignedChunkReader(filePath, id, startChunk, endChunk, chunkSize)
        val read = reader.readChunk().replaceAll("\n", " ")
        println(s"$id=$read")
      }
    }
  }

}