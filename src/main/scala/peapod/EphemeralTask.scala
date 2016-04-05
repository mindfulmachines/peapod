package peapod

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

abstract class EphemeralTask[V: ClassTag](implicit p: Peapod)
  extends Task[V] with Logging {

  protected def generate: V

  protected[peapod] def  build(): V = {
    logInfo("Loading" + dir)
    generate
  }
  def exists(): Boolean = false
}
