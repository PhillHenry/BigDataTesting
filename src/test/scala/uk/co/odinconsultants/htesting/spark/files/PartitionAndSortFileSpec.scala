package uk.co.odinconsultants.htesting.spark.files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.spark.sql.{DataFrame, Dataset}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting._
import uk.co.odinconsultants.htesting.spark.SparkForTesting._

@RunWith(classOf[JUnitRunner])
class PartitionAndSortFileSpec extends WordSpec with Matchers {

  "partition and sorting" should {
    "be seen in parquet files" in {
      import session.implicits._
      val nSlots        = 3
      val nPartDistinct = 2
      val data          = (1 to 10000).map(i => (i, (('A' + i % 25).toString * 1024).toString, i % nPartDistinct, i % nSlots))
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

//      df.sort(intKey).write.partitionBy(partitionkey).parquet(filename)

      // Basically, with this amount of data, up to 400 files are created with each one having max(intKey) == min(intKey)
      // Over 400 files, and then we see contiguous intKey values starting to creep in.
      df.sort(intKey).write.partitionBy(partitionkey).parquet(filename)

      val files = list(filename).filter(_.toString.endsWith(".parquet"))
      println("Files:\n" + files.mkString("\n"))

      val fromHdfs = session.read.parquet(filename)

      println("Schema: ")
      fromHdfs.printSchema() // Note the schema changes. The partition key goes to the end of the list of columns
      files.length shouldBe > (1)
      files should have length (nSlots * nPartDistinct) // works when (nSlots * nPartDistinct) <= 400

      val minMaxs = files.map { file =>
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
        val singleDF = session.read.parquet(file.toString)

        // see https://stackoverflow.com/questions/45178991/calculate-min-max-using-spark-dataframe-and-vertically-align-output
        val description = singleDF.describe()
        description.show() // this indeed does show the stats
        val min = description.where("summary == 'min'").select(intKey).collect()(0)
        val max = description.where("summary == 'max'").select(intKey).collect()(0)
        (min, max)
      }

      println(s"PH: Min/max of column $intKey: ${minMaxs.mkString(", ")}")
      println(s"PH: where min != max: ${minMaxs.filter { case(min, max) => min != max} .mkString(", ")}")


//      Thread.sleep(Long.MaxValue)

      checkOrdered(fromHdfs, intKey, nSlots, nPartDistinct)
      checkOrdered(df, intKey, nSlots, nPartDistinct)
//      checkOrdered(data.toDF("id", text, partitionkey, intKey), intKey, nSlots, index = 3) // this blows up though. Seems DataFrame needs to be persisted for the sort to take effect

      val query = fromHdfs.where(s"$intKey == 3")
      query.explain()
      /*
== Physical Plan ==
*(1) Project [id#21, text#22, intkey#23, partitionkey#24]
+- *(1) Filter (isnotnull(intkey#23) && (intkey#23 = 3))
   +- *(1) FileScan parquet [id#21,text#22,intkey#23,partitionkey#24] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://127.0.0.1:41684/1544107487759], PartitionCount: 2, PartitionFilters: [], PushedFilters: [IsNotNull(intkey), EqualTo(intkey,3)], ReadSchema: struct<id:int,text:string,intkey:int>

Note that the PushedFilters shows we're using Predicate Pushdown
       */
      query.show()

      val otherDf = (1 to 1000).map(i => (i, (('A' + i % 25).toString * 1024).toString, i % nPartDistinct, i % nSlots)).toDF("id", text, partitionkey, intKey).repartition(4)
      println(s"Number of partitions = ${df.rdd.partitions.length} and ${otherDf.rdd.partitions.length}")
      println(s"Number of partitions in cross joins = ${df.crossJoin(otherDf).rdd.partitions.length}")
    }
  }

  def checkOrdered(df: DataFrame, intKey: String, nSlots: Int, nPartDistinct: Int, index: Int = 2): Unit = {
    import df.sqlContext.implicits._
    val intsPerPartition = df.mapPartitions { xs =>
      val ids = xs.map { _.getInt(index) }.toSet
      Iterator(ids)
    }

    println(intKey + ": " + df.map {_.getInt(index) }.distinct().collect().mkString(", "))

    val inMem: Array[Set[Int]] = intsPerPartition.collect()
    val nPartitions = df.rdd.partitions.size
    val expectedNumPerSlot = math.max((nSlots * nPartDistinct) / nPartitions, 1)
    println(s"number of partitions = $nPartitions, expected number per slot = $expectedNumPerSlot, sizes = ${inMem.map(_.size).mkString(", ")}\n")
    withClue(s"ints in a given partition:\n${inMem.mkString("\n")}\nExpected number = $expectedNumPerSlot\nNum of partitions = $nPartitions") {
      inMem.foreach { _.size shouldBe expectedNumPerSlot +- expectedNumPerSlot.toInt }
    }
  }

}
