package peapod

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.MD5Hash

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

abstract class Task [+T: ClassTag](implicit p: Peapod) extends BaseTask {
  protected lazy val pea: Pea[T] = p.pea[T](this)
  protected lazy val dir = p.path + "/" + name + "/" + recursiveVersionShort

  protected[peapod] def build(): T

  protected def pea[D: ClassTag](t: Task[D]): Pea[D] = {
    val child = p.pea(t)
    pea.addChild(child)
    child.addParent(pea)
    child
  }

  def exists(): Boolean

  def recursiveVersionShort: String = {
    pea.recursiveVersionShort
  }
}
