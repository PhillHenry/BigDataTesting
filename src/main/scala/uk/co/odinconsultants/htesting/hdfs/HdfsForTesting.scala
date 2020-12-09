package uk.co.odinconsultants.htesting.hdfs

import java.io.{BufferedInputStream, BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import uk.co.odinconsultants.htesting.local.TestingFileUtils.tmpDirectory
import uk.co.odinconsultants.htesting.log.Logging

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object HdfsForTesting extends Logging {

  val baseDir = tmpDirectory("tests").getAbsoluteFile
  val conf = new Configuration()
  // see https://stackoverflow.com/questions/43180305/cannot-connect-to-hive-using-beeline-user-root-cannot-impersonate-anonymous
  conf.set("hadoop.proxyuser.hive.groups",        "*")
  conf.set("hadoop.proxyuser.hive.hosts",         "*")
  val user = System.getProperty("user.name")
  conf.set(s"hadoop.proxyuser.$user.groups",      "*")
  conf.set(s"hadoop.proxyuser.$user.hosts",       "*")
  conf.set("fs.file.impl",                        classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  conf.set("fs.hdfs.impl",                        classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  val path: String = baseDir.getAbsolutePath
  info(s"path = $path")
  conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, path)
  val builder = new MiniDFSCluster.Builder(conf)

  info("Attempting to start HDFS")
  val hdfsCluster = builder.build()
  val distributedFS = hdfsCluster.getFileSystem
  val hdfsUri = "hdfs://127.0.0.1:" + hdfsCluster.getNameNodePort + "/"

  def readAsString(path: String): String = {
    val dis       = distributedFS.open(new Path(path))
    val buffered  = new BufferedReader(new InputStreamReader(dis))

    @tailrec
    def readFile(acc: String, line: String): String = {
      if (line == null) {
        acc
      } else {
        readFile(acc + line, buffered.readLine())
      }
    }

    val contents = readFile("", buffered.readLine())
    buffered.close()
    contents
  }

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
