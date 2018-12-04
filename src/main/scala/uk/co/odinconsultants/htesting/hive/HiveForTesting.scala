package uk.co.odinconsultants.htesting.hive

import java.io.File.separator

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.tools.MetastoreSchemaTool
import org.apache.hive.service.cli.CLIService
import org.apache.hive.service.server.HiveServer2
import uk.co.odinconsultants.htesting.local.TestingFileUtils.tmpDirectory
import uk.co.odinconsultants.htesting.local.{PortUtils, TestingFileUtils}
import uk.co.odinconsultants.htesting.log.Logging
import uk.co.odinconsultants.htesting.spark.SparkForTesting

object HiveForTesting extends Logging {

  info("WARNING! This test harness does not (yet) respect the metastore")

  val hiveThriftPort = PortUtils()
  info(s"thrift port = $hiveThriftPort")

  val hiveConf    = new HiveConf()
  hiveConf.set("datanucleus.schema.autoCreateAll",    "false")
  hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_ENABLE, true)
  val webGui = PortUtils()
  info(s"Hive web gui running on port $webGui")
  hiveConf.setIntVar(ConfVars.HIVE_SERVER2_WEBUI_PORT, webGui)
  // see https://stackoverflow.com/questions/43180305/cannot-connect-to-hive-using-beeline-user-root-cannot-impersonate-anonymous
  hiveConf.set("hadoop.proxyuser.hive.groups",        "*")
  hiveConf.set("hadoop.proxyuser.hive.hosts",         "*")
  hiveConf.set("hive.stats.autogather",               "false")
  hiveConf.set("hive.log.explain.output",             "true")

  hiveConf.set("spark.driver.allowMultipleContexts",  "true")

  hiveConf.set("hive.metastore.schema.verification",  "false")
  hiveConf.set("hive.execution.engine",               "spark")
  hiveConf.set("hive.server2.thrift.port",            hiveThriftPort.toString)
  hiveConf.set("spark.master",                        SparkForTesting.master)
  info("Hive conf: " + hiveConf.getAllProperties)

  val derbyHome: String = tmpDirectory("derby").getAbsolutePath
  info(s"Setting Derby Home to be $derbyHome")
  System.setProperty("derby.system.home", derbyHome)

  SparkHacks.hackSparkSubmitClass()

  val metaStoreDir = {
    HiveForTesting.getClass.getResource("/").getPath + s"${separator}..${separator}..${separator}src${separator}main${separator}resources${separator}metastore${separator}"
  }
  info("Using metastore directory: " + metaStoreDir)
  MetastoreSchemaTool.homeDir = metaStoreDir
  assert(MetastoreSchemaTool.run("--verbose -initSchema -dbType derby".split(" ")) == 0)

  val hiveServer  = new HiveServer2()
  hiveServer.init(hiveConf)
  hiveServer.start()


  val hiveCli: CLIService = {
    import scala.collection.JavaConversions._
    var cli: CLIService = null
    for (service <- hiveServer.getServices) {
      if (service.isInstanceOf[CLIService]) cli = service.asInstanceOf[CLIService]
    }
    cli
  }

  Class.forName("org.apache.hive.jdbc.HiveDriver")

  def main(args: Array[String]): Unit = { // smoke test
    info("Hive started. CLI = " + hiveCli)
    System.exit(0)
  }

}
