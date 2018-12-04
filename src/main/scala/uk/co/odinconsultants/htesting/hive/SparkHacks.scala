package uk.co.odinconsultants.htesting.hive

import uk.co.odinconsultants.htesting.log.Logging

object SparkHacks extends Logging {

  def hackSparkSubmitClass(): Class[_] = {
    val sparkSubmitClass: Class[_] = Class.forName("org.apache.spark.deploy.SparkSubmit")
    sparkSubmitClass.getDeclaredConstructors.foreach(_.setAccessible(true))
    sparkSubmitClass
  }

  def main(args: Array[String]): Unit = {
    info(hackSparkSubmitClass().newInstance())
  }

}
