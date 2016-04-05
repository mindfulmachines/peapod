package peapod

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.MD5Hash

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by marcin.mejran on 4/1/16.
  */
abstract class BaseTask(implicit val p: Peapod)  {
  lazy val baseName: String = this.getClass.getName
  lazy val name: String = baseName
  lazy val versionName: String = name

  val version: String = "1"
  protected val dir: String

  def exists(): Boolean

  def recursiveVersionShort(): String
}
