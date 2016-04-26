package peapod

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.MD5Hash

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

abstract class Task [+T: ClassTag] {
  val p: Peapod
  lazy val baseName: String = this.getClass.getName
  lazy val name: String = baseName
  lazy val versionName: String = name

  val storable: Boolean

  val version: String = "1"

  protected lazy val dir = p.path + "/" + name + "/" + recursiveVersionShort

  var children: List[Task[_]] = Nil

  protected[peapod] def build(): T

  protected def pea[D: ClassTag](t: Task[D]): Task[D] = {
    val child = t
    children = children :+ child
    child
  }

  def exists(): Boolean

  lazy val recursiveVersion: List[String] = {
    //Sorting first so that changed in ordering of peas doesn't cause new version
    versionName + ":" + version :: children.toList.sortBy(_.versionName).flatMap(_.recursiveVersion.map("-" + _)).toList
  }

  def recursiveVersionShort: String = {
    val bytes = MD5Hash.digest(recursiveVersion.mkString("\n")).getDigest
    val encodedBytes = Base64.encodeBase64URLSafeString(bytes)
    new String(encodedBytes)
  }


  override def toString: String = {
    name
  }


  override def hashCode: Int = {
    toString.hashCode
  }
}
