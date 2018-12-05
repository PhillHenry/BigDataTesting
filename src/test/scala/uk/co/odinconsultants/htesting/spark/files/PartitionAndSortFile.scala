package uk.co.odinconsultants.htesting.spark.files

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
      val data          = (1 to 10000).map(i => (i, ('A' + i % 25) + 10240, i % 2))
      val partitionkey  = "partitionkey"
      val text          = "text"
      val df            = data.toDF("id", text, partitionkey)

      val filename = hdfsUri + System.currentTimeMillis()

      // 'save' does not support bucketBy right now;
//      df.write.partitionBy(partitionkey).bucketBy(10, text).parquet(filename)

      // sortBy must be used together with bucketBy;
//      df.write.partitionBy(partitionkey).sortBy(text).parquet(filename)

      // 'save' does not support bucketBy and sortBy right now;
//      df.write.partitionBy(partitionkey).sortBy(text).bucketBy(10, text).parquet(filename)


      df.sort(text).write.partitionBy(partitionkey).parquet(filename)


      println("Files:\n" + list(filename).mkString("\n"))
    }
  }

}
