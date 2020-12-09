package uk.co.odinconsultants.htesting.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkForTesting {

  import uk.co.odinconsultants.htesting.hdfs.HadoopForTesting._

  val master: String          = "local[*]"
  val sparkConf: SparkConf    = new SparkConf().setMaster(master).setAppName("Tests").set("spark.driver.allowMultipleContexts", "true")
  sparkConf.set("spark.driver.allowMultipleContexts", "true")
  val sc: SparkContext        = SparkContext.getOrCreate(sparkConf)
  val session: SparkSession   = SparkSession.builder().getOrCreate()
  val sqlContext: SQLContext  = session.sqlContext

}
