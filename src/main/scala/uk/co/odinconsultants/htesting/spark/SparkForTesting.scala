package uk.co.odinconsultants.htesting.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkForTesting {

  import co.uk.odinconsultants.spark.HadoopForTesting._

  info("Using binaries in " + WINDOWS_BINARY_DIRECTORY)

  val master: String          = "local[*]"
  val sparkConf: SparkConf    = new SparkConf().setMaster(master).setAppName("Tests")
  sparkConf.set("spark.driver.allowMultipleContexts", "true")
  val sc: SparkContext        = SparkContext.getOrCreate(sparkConf)
  val session: SparkSession   = SparkSession.builder().getOrCreate()
  val sqlContext: SQLContext  = session.sqlContext

}
