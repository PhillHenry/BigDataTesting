package uk.co.odinconsultants.htesting.hive

object SparkHacks {

  def hackSparkSubmitClass(): Class[_] = {
    val sparkSubmitClass: Class[_] = Class.forName("org.apache.spark.deploy.SparkSubmit")
    sparkSubmitClass.getDeclaredConstructors.foreach(_.setAccessible(true))
    sparkSubmitClass
  }

  def main(args: Array[String]): Unit = {
    println(hackSparkSubmitClass().newInstance())
  }

}
