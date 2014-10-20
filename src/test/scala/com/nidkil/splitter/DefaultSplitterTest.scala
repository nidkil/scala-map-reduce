package com.nidkil.splitter

import java.io.File
import java.io.IOException
import java.io.RandomAccessFile

import org.scalatest.FunSpec
import org.scalatest.Matchers

class DefaultSplitterTest extends FunSpec with Matchers {

  import DefaultSplitter._

  def generateTestFile(size: Long) : File = {
    val tempFile = File.createTempFile("test_", ".tmp")
    val file = new RandomAccessFile(tempFile, "rw");
    try {
      file.setLength(size);
    } finally {
      file.close
    }
    tempFile
  }

  describe("A DefaultSplitterTest") {
    it("should throw an exception when the file does not exist") {
      val splitter = new DefaultSplitter()
      intercept[IOException] {
        splitter.split(new File("does_not_exist"))
      }
    }
    it("should return 5 chunks when the max chunk size is not changed") {
      // Generate a 20 MB file
      val tempFile = generateTestFile(1024 * 20)
      val splitter = new DefaultSplitter()
      val chunks = splitter.split(tempFile)
      assert(chunks.size == 4)      
      tempFile.delete
    }
    it("should return 6 chunks when the max chunk size is not changed") {
      // Generate a 22 MB file
      val tempFile = generateTestFile(1024 * 22)
      val splitter = new DefaultSplitter()
      val chunks = splitter.split(tempFile)
      assert(chunks.size == 5)      
      tempFile.delete
      info("the last chunk should have a size of 2 MB " + chunks)
      assert(chunks.last.size == 1024 * 2)
    }
    it("return 10 chunks when the max chunk size is changed to 2 MB") {
      // Generate a 20 MB file
      val tempFile = generateTestFile(1024 * 20)
      val splitter = new DefaultSplitter()
      // Max chunk size 2 MB
      splitter.maxChunkSize(1024 * 2)
      val chunks = splitter.split(tempFile)
      assert(chunks.size == 10)      
      tempFile.delete
    }
    it("should return 1 chunk when the file size is smaller than the max chunk size") {
      // Generate a 4 MB file
      val tempFile = generateTestFile(1024 * 4)
      val splitter = new DefaultSplitter()
      val chunks = splitter.split(tempFile)
      assert(chunks.size == 1)      
      tempFile.delete
    }
    it("should return no chunks when the file size is zero") {
      // Generate a 4 MB file
      val tempFile = generateTestFile(0)
      val splitter = new DefaultSplitter()
      val chunks = splitter.split(tempFile)
      assert(chunks.size == 0)      
      tempFile.delete
    }
  }

}