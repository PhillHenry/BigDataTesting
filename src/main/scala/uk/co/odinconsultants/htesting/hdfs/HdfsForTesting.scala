package uk.co.odinconsultants.htesting.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import uk.co.odinconsultants.htesting.local.TestingFileUtils.tmpDirectory
import uk.co.odinconsultants.htesting.log.Logging

import scala.collection.mutable.ArrayBuffer

object HdfsForTesting extends Logging {

  val baseDir = tmpDirectory("tests").getAbsoluteFile
  val conf = new Configuration()
  // see https://stackoverflow.com/questions/43180305/cannot-connect-to-hive-using-beeline-user-root-cannot-impersonate-anonymous
  conf.set("hadoop.proxyuser.hive.groups",        "*")
  conf.set("hadoop.proxyuser.hive.hosts",         "*")
  val user = System.getProperty("user.name")
  conf.set(s"hadoop.proxyuser.$user.groups",        "*")
  conf.set(s"hadoop.proxyuser.$user.hosts",        "*")
  conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
  val builder = new MiniDFSCluster.Builder(conf)

  info("Attempting to start HDFS")
  val hdfsCluster = builder.build()
  val distributedFS = hdfsCluster.getFileSystem
  val hdfsUri = "hdfs://127.0.0.1:" + hdfsCluster.getNameNodePort + "/"

  def list(path: String): List[Path] = {
    info(s"Looking in $path")

    val files = distributedFS.listFiles(new Path(path), true)

    val allPaths = ArrayBuffer[Path]()
    while (files.hasNext) {
      val file = files.next
      allPaths += file.getPath
    }

    allPaths.toList
  }

}
