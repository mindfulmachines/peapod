package generic

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.Path
import peapod.Peapod

import scala.util.Random

/**
  * Created by Marcin on 6/15/2016.
  */
object PeapodGenerator {
  def peapod() = {
    val sdf = new SimpleDateFormat("ddMMyy-hhmmss")
    val path = System.getProperty("java.io.tmpdir") + "workflow-" + sdf.format(new Date()) + Random.nextInt()
    new File(path).mkdir()
    new File(path).deleteOnExit()
    val w = new Peapod(
      path= new Path("file://",path.replace("\\","/")).toString,
      raw="")(generic.Spark.sc)
    w
  }
  def peapodNonRecursive() = {
    val sdf = new SimpleDateFormat("ddMMyy-hhmmss")
    val path = System.getProperty("java.io.tmpdir") + "workflow-" + sdf.format(new Date()) + Random.nextInt()
    new File(path).mkdir()
    new File(path).deleteOnExit()
    val w = new Peapod(
      path= new Path("file://",path.replace("\\","/")).toString,
      raw="")(generic.Spark.sc) {
      override val recursiveVersioning = false
    }
    w
  }
}
