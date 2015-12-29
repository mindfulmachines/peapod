package dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

abstract class EphemeralTask[V: ClassTag](implicit val p: Peapod)
  extends Task[V] {

  protected def generate: V

  protected[dependency] def  build(): V = {
    println("Loading" + dir)
    val generated = generate
    if(shouldPersist()) {
      println("Loading" + dir + " Persisting")
      generated match {
        case g: RDD[_] => g.persist(StorageLevel.MEMORY_AND_DISK).asInstanceOf[V]
        case g: DataFrame => g.cache().asInstanceOf[V]
        case _ => generated
      }
    } else {
      generated
    }
  }
  def exists(): Boolean = false
}
