package com.nidkil.utils

import java.io.File
import java.io.FileWriter
import java.io.IOException
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FunSpec
import org.scalatest.Matchers
import com.nidkil.mapreduce.Chunk
import com.nidkil.splitter.DefaultSplitter
import scala.collection.mutable.LinkedHashSet
import org.scalatest.BeforeAndAfterAll

case class Test(id: Int, maxChunkSize: Int, numResults: Int)

class WordBoundryChunkReaderTest extends FunSpec with Matchers with BeforeAndAfterAll {

  val testData = """This is a test sentence to test the WordAlignedChunkReader and see if it cuts
of the lines in the correct position. We also need to see if the whole file is
processed. A very exiting test. At least I think it is as this has been bugging
me for a while."""

  val testValues = List[Test](
    new Test(1, 20, 13),
    new Test(2, 25, 11),
    new Test(3, 30, 9),
    new Test(4, 35, 8),
    new Test(5, 40, 7),
    new Test(6, 45, 6),
    new Test(7, 50, 6),
    new Test(8, 55, 5),
    new Test(9, 60, 5),
    new Test(10, 65, 4),
    new Test(11, 70, 4),
    new Test(12, 75, 4),
    new Test(13, 80, 4),
    new Test(14, 85, 3),
    new Test(15, 90, 3),
    new Test(16, 95, 3),
    new Test(17, 100, 3),
    new Test(18, 120, 3),
    new Test(19, 140, 2),
    new Test(20, 160, 2),
    new Test(21, 180, 2),
    new Test(22, 200, 2),
    new Test(23, 300, 1))
  val fileName = "test.dat"
  val baseDir = new File(System.getProperty("java.io.tmpdir"))
  val file = new File(baseDir, fileName)

  private def printToFile(f: File)(data: String) {
    val fileWriter = new FileWriter(f)
    try {
      fileWriter.write(data)
    } finally {
      fileWriter.close()
    }
  }

  override def beforeAll() {
    printToFile(file) { testData }
    info(s"file is ${file.getAbsolutePath()} [size=${file.length()}]")
  }

  override def afterAll() {
    file.delete
  }

  def printResults(testValueId: Int, results: LinkedHashSet[String]) {
    println(s" %%%% [$testValueId]")
    var i = 0
    results.foreach { r => i += 1; println(s" * $i: ${r.replaceAll("(\\r|\\n)", " ")} [cnt=${r.split(" ").length}, length=${r.length}]") }
    println(s" %%%% [$testValueId]")
  }

  def handleSplit(maxChunkSize: Int): LinkedHashSet[String] = {
    val splitter = new DefaultSplitter()
    splitter.maxChunkSize(maxChunkSize)
    val chunks = splitter.split(file)
    var results = LinkedHashSet[String]()

    chunks.foreach { chunk =>
      val reader = new WordBoundryChunkReader(chunk)
      results += reader.readChunk()
    }

    results
  }

  describe("A WordBoundryChunkReader") {
    it("should throw an exception when the file does not exist") {
      intercept[IOException] {
        new WordBoundryChunkReader(new Chunk(1, "does_not_exist", 0, 100, 50))
      }
    }
    it(s"read the chunks of a file making sure that each chunk is aligned with the boundry of the first and last word of the chunk") {
      var i = 1
      for (t <- testValues) {
        val results = handleSplit(t.maxChunkSize)

        info(s"$i. Testing maxChunkSize=${t.maxChunkSize} should return ${t.numResults} results")

        assert(results.size == t.numResults)

        i += 1
      }
    }
  }

}