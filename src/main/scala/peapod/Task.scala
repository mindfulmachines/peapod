package peapod

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.MD5Hash

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

abstract class Task [+T: ClassTag] {

  lazy val baseName: String = this.getClass.getName
  lazy val name: String = baseName
  lazy val versionName: String = name

  val version: String = "1"

  def dir(p: Peapod) = p.path + "/" + name + "/" + recursiveVersionShort(p)

  var children: List[Peapod => Pea[_]] = Nil

  protected[peapod] def build(p: Peapod): T

  protected def pea[D: ClassTag](t: Task[D]): Peapod => Pea[D] = {
    val child = {p: Peapod => p.pea(t)}
    children = children :+ child
    child
  }

  def exists(p: Peapod): Boolean

  def recursiveVersionShort(p: Peapod): String = {
    p.pea(this).recursiveVersionShort
  }
}
