package uk.co.odinconsultants.htesting.log

trait Logging {

  def info(x: Any): Unit = println(s"PH: $x")

}
