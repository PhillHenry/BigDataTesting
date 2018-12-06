package uk.co.odinconsultants.htesting.spark.files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting._
import uk.co.odinconsultants.htesting.spark.SparkForTesting._

@RunWith(classOf[JUnitRunner])
class PartitionAndSortFile extends WordSpec with Matchers {

  "partition and sorting" should {
    "be seen in parquet files" in {
      import session.implicits._
      val nSlots        = 200
      val data          = (1 to 10000).map(i => (i, (('A' + i % 25).toString * 1024).toString, i % 2, i % nSlots))
      println("data head: " + data.head)
      val partitionkey  = "partitionkey"
      val text          = "text"
      val intKey        = "intkey"
      val df            = data.toDF("id", text, partitionkey, intKey)

      val filename = hdfsUri + System.currentTimeMillis()

      // 'save' does not support bucketBy right now;
//      df.write.partitionBy(partitionkey).bucketBy(10, text).parquet(filename)

      // sortBy must be used together with bucketBy;
//      df.write.partitionBy(partitionkey).sortBy(text).parquet(filename)

      // 'save' does not support bucketBy and sortBy right now;
//      df.write.partitionBy(partitionkey).sortBy(text).bucketBy(10, text).parquet(filename)


      df.sort(intKey).write.partitionBy(partitionkey).parquet(filename)

      val files = list(filename).filter(_.toString.endsWith(".parquet"))
      println("Files:\n" + files.mkString("\n"))

      val fromHdfs = session.read.parquet(filename)

      println("Schema: ")
      fromHdfs.printSchema() // Note the schema changes. The partition key goes to the end of the list of columns


      files.foreach { file =>
        val conf = new Configuration()
        conf.setBoolean("parquet.strings.signed-min-max.enabled", true)
        val reader = ParquetFileReader.readFooter(conf, file, ParquetMetadataConverter.NO_FILTER)
        println("Reader = " + reader)
        import scala.collection.JavaConversions._
        import scala.collection.JavaConverters._
        println("blocks = " + reader.getBlocks)
        println("blocks = " + reader.getBlocks.toList.mkString("\n"))
        println("stats:")
        reader.getBlocks.toList.foreach(blockMetaData => println(blockMetaData.getColumns.toList.foreach(_.getStatistics)))
        println("Columns")
        reader.getBlocks.toList.foreach(blockMetaData => println(blockMetaData.getColumns.toList.mkString("\n")))
      }

//      Thread.sleep(Long.MaxValue)


      val intsPerPartition = fromHdfs.mapPartitions { xs =>
        val ids = xs.map { _.getInt(2) }.toSet
        Iterator(ids)
      }

      println(intKey + ": " + fromHdfs.map {_.getInt(2) }.distinct().collect().mkString(", "))

      val inMem: Array[Set[Int]] = intsPerPartition.collect()
      val nPartitions = fromHdfs.rdd.partitions.size
      val expectedNumPerSlot = nSlots / nPartitions
      println(s"number of partitions = $nPartitions, expected number per slot = $expectedNumPerSlot, sizes = ${inMem.map(_.size).mkString(", ")}")
      withClue(s"ints in a given partition:\n${inMem.mkString("\n")}\nExpected number = $expectedNumPerSlot\nNum of partitions = $nPartitions") {
        inMem.foreach { _.size shouldBe expectedNumPerSlot +- expectedNumPerSlot.toInt }
      }
    }
  }

}
