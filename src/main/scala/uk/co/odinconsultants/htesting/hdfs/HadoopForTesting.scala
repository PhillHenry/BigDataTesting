package uk.co.odinconsultants.htesting.hdfs

import uk.co.odinconsultants.htesting.log.Logging

class HadoopForTesting

object HadoopForTesting extends Logging {

  def error(x: String): Unit = System.err.println(x)


  if (System.getProperty("os.name").toLowerCase.indexOf("win") != -1) {
    info("Windows systems need to have Hadoop installed and configured for these tests to run.")
  } else {
    info("Not a windows system, not using binaries")
  }

}
