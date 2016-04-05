package peapod

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.MD5Hash

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

abstract class Task [+T: ClassTag](implicit p: Peapod) {
  protected lazy val pea: Pea[T] = p.pea[T](this)

  protected[peapod] def build(): T

  protected def pea[D: ClassTag](t: Task[D]): Pea[D] = {
    val child = p.pea(t)
    pea.addChild(child)
    child.addParent(pea)
    child
  }

  def exists(): Boolean

}



object Task {
  implicit def getAnyTask[T](task: Task[T]): T =
    task.get()
}


