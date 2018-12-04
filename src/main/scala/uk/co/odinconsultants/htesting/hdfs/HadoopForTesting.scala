package co.uk.odinconsultants.spark

import java.io.File.separator

class HadoopForTesting

object HadoopForTesting {

  val BINARY_DIRECTORY: String = {
    val location    = classOf[HadoopForTesting].getProtectionDomain.getCodeSource.getLocation.getFile.replace("/", separator)
    val minusTarge  = location.substring(0, location.indexOf("target"))
    val path        = (minusTarge + "src" + separator + "test" + separator + "resources" + separator).substring(1)
    println(s"PH: path = $path")
    path
  }

  if (System.getProperty("os.name").toLowerCase.indexOf("win") != -1) {
    println("PH: setting properties")
    System.setProperty("java.library.path", BINARY_DIRECTORY )
    System.setProperty("hadoop.home.dir",   BINARY_DIRECTORY)

    val classLoader = this.getClass.getClassLoader
    val field       = classOf[ClassLoader].getDeclaredField("usr_paths")
    field.setAccessible(true)
    val usrPath     = field.get(classLoader).asInstanceOf[Array[String]]
    val newUsrPath  = new Array[String](usrPath.length + 1)
    System.arraycopy(usrPath, 0, newUsrPath, 0, usrPath.length)
    newUsrPath(usrPath.length) = BINARY_DIRECTORY  + separator + "bin" + separator
    field.set(classLoader, newUsrPath)

    val field_system_loaded = classOf[org.apache.hadoop.fs.FileSystem].getDeclaredField("FILE_SYSTEMS_LOADED")
    field_system_loaded.setAccessible(true)
    field_system_loaded.setBoolean(null, true)

    val nativeCodeLoadedField = classOf[org.apache.hadoop.util.NativeCodeLoader].getDeclaredField("nativeCodeLoaded")
    nativeCodeLoadedField.setAccessible(true)
    nativeCodeLoadedField.set(null, false)
  } else {
    println("PH: Not a windows system, not using binaries")
  }

  def main(args: Array[String]): Unit = {
    println(BINARY_DIRECTORY)
  }
}
