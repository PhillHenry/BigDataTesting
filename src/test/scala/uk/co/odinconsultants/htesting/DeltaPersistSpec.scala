package uk.co.odinconsultants.htesting

import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.htesting.spark.SparkForTesting

class DeltaPersistSpec extends WordSpec with Matchers {

  "A typical dataframe" should {
    "be persisted as a delta frame" ignore {
      val s               = SparkForTesting.session

      val filename  = hdfsUri + this.getClass.getSimpleName

      val first: Seq[String]        = "the quick brown fox jumped over the lazy dog".split(" ")
      deltaWrite(s, first, filename)

      val fromDisk = s.read.parquet(filename)
      fromDisk.collect().map(_.mkString("")).toSet shouldBe first.toSet
    }
  }

  private def deltaWrite(s: SparkSession, xs: Seq[String], filename: String): Unit = {
    import s.implicits._
    val df = s.sparkContext.parallelize(xs).toDF("values")
    df.write.format("delta").mode("overwrite").save(filename)
  }
}
