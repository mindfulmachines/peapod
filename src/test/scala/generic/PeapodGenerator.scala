package generic

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import com.google.common.io.Resources
import org.apache.hadoop.fs.{FileSystem, Path}
import peapod.{Peapod, Web}

import scala.util.Random

object PeapodGenerator {
  def createTempDir(): String = {
    val sdf = new SimpleDateFormat("ddMMyy-hhmmss")
    val rawPath = System.getProperty("java.io.tmpdir") + "workflow-" + sdf.format(new Date()) + Random.nextInt()
    val path = new Path("file://",rawPath.replace("\\","/")).toString
    val fs = FileSystem.get(new URI(path), SparkS3.sc.hadoopConfiguration)
    fs.mkdirs(new Path(path))
    fs.deleteOnExit(new Path(path))
    path
  }

  def peapod() = {
    val path = createTempDir()
    val w = new Peapod(
      path= path,
      raw="file://" + Resources.getResource("raw").getPath)(generic.Spark.sc)
    w
  }
  def peapodNonRecursive() = {
    val path = createTempDir()
    val w = new Peapod(
      path= path,
      raw="file://" + Resources.getResource("raw").getPath)(generic.Spark.sc) {
      override val recursiveVersioning = false
    }
    w
  }
  def web() = {
    val path = createTempDir()
    val w = new Peapod(
      path= path,
      raw="file://" + Resources.getResource("raw").getPath)(generic.Spark.sc) with Web
    w
  }
}
