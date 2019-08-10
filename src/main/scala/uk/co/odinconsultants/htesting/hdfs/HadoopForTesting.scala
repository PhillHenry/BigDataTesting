package uk.co.odinconsultants.htesting.hdfs

import java.io.File.separator

import uk.co.odinconsultants.htesting.log.Logging

import scala.util.{Failure, Success, Try}

class HadoopForTesting

object HadoopForTesting extends Logging {

  val WINDOWS_BINARY_DIRECTORY: String = {
    Try {
      val location = classOf[HadoopForTesting].getProtectionDomain.getCodeSource.getLocation.getFile.replace("/", separator)
      val minusTarge = location.substring(0, location.indexOf("target"))
      val path = (minusTarge + "src" + separator + "main" + separator + "resources" + separator).substring(1)
      info(s"PH: path = $path")
      path
    } match {
      case Success(x) => x
      case Failure(x) => x.getMessage
    }
  }

  if (System.getProperty("os.name").toLowerCase.indexOf("win") != -1) {
    info("PH: setting properties")
    System.setProperty("java.library.path", WINDOWS_BINARY_DIRECTORY )
    System.setProperty("hadoop.home.dir",   WINDOWS_BINARY_DIRECTORY)

    val classLoader = this.getClass.getClassLoader
    val field       = classOf[ClassLoader].getDeclaredField("usr_paths")
    field.setAccessible(true)
    val usrPath     = field.get(classLoader).asInstanceOf[Array[String]]
    val newUsrPath  = new Array[String](usrPath.length + 1)
    System.arraycopy(usrPath, 0, newUsrPath, 0, usrPath.length)
    newUsrPath(usrPath.length) = WINDOWS_BINARY_DIRECTORY  + separator + "bin" + separator
    field.set(classLoader, newUsrPath)

    val field_system_loaded = classOf[org.apache.hadoop.fs.FileSystem].getDeclaredField("FILE_SYSTEMS_LOADED")
    field_system_loaded.setAccessible(true)
    field_system_loaded.setBoolean(null, true)

    val nativeCodeLoadedField = classOf[org.apache.hadoop.util.NativeCodeLoader].getDeclaredField("nativeCodeLoaded")
    nativeCodeLoadedField.setAccessible(true)
    nativeCodeLoadedField.set(null, false)
  } else {
    info("PH: Not a windows system, not using binaries")
  }

  def main(args: Array[String]): Unit = {
    info(WINDOWS_BINARY_DIRECTORY)
  }
}
