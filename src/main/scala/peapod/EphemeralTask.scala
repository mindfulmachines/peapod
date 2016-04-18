package peapod

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

abstract class EphemeralTask[V: ClassTag]
  extends Task[V] with Logging {

  protected def generate(p: Peapod): V

  protected[peapod] def  build(p: Peapod): V = {
    logInfo("Loading" + dir(p))
    generate(p)
  }
  def exists(p: Peapod): Boolean = false
}
