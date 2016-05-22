package peapod

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.MD5Hash

import scala.reflect.ClassTag

abstract class Task [+T: ClassTag] extends Versioned {
  val p: Peapod

  lazy val baseName: String = this.getClass.getName
  lazy val name: String = baseName
  lazy val versionName: String = name
  /**
    * Short description of what this task does
    */
  val description: String = ""

  val storable: Boolean

  protected lazy val dir = p.path + "/" + name + "/" + recursiveVersionShort

  var children: List[Task[_]] = Nil

  def build(): T

  protected def pea[D: ClassTag](t: Task[D]): Task[D] = {
    val child = t
    children = children :+ child
    child
  }

  def exists(): Boolean

  def delete()

  def load() : T

  lazy val recursiveVersion: List[String] = {
    //Sorting first so that changed in ordering of peas doesn't cause new version
    versionName + ":" + version :: children.toList.sortBy(_.versionName).flatMap(_.recursiveVersion.map("-" + _)).toList
  }

  def recursiveVersionShort: String = {
    if(p.recursiveVersioning) {
      val bytes = MD5Hash.digest(recursiveVersion.mkString("\n")).getDigest
      val encodedBytes = Base64.encodeBase64URLSafeString(bytes)
      new String(encodedBytes)
    } else {
      "latest"
    }
  }


  /**
    * Generates a string representation of the metadata for this task including name, version, description and these
    * for all Tasks that this task is dependent on
    *
    * @return String representation of the metadata of this task
    */
  def metadata(): String = {
    val allChildren = children.flatMap(_.children).distinct
    val out =
      description match {
        case "" => name + ":" + version :: Nil
        case _ => name + ":" + version ::
          description :: Nil
      }

    val childMetadata =   allChildren.flatMap{t => t.description match {
      case "" =>
        "-" + t.name + ":" + t.version :: Nil
      case _ => "-" + t.name + ":" + t.version ::
        "--" + t.description ::
        Nil
    }}
    (out ::: childMetadata).mkString("\n")
  }


  override def toString: String = {
    name
  }


  override def hashCode: Int = {
    toString.hashCode
  }

  override def equals(o: Any) = {
    o match {
      case t: Task[_] => t.toString == this.toString
      case _ => false
    }
  }
}

