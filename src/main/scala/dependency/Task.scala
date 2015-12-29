package dependency

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.MD5Hash

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

abstract class Task [T: ClassTag](implicit p: Peapod) {
  lazy val name: String = this.getClass.getName
  protected val version: String = "1"
  protected val dependencies= scala.collection.mutable.ArrayBuffer.empty[Task[_]]

  protected lazy val dir = p.fs + p.path + "/" + name + "/" + recursiveVersionShort

  def get(): T = {
    //Adds all dependencies to workflow
    dependencies.foreach(d => p.put(this, d))
    //Builds all dependencies
    if(! exists()) {
      dependencies.foreach(d => p.build(d))
    }
    val f= p.build(this)
    val t= Await.result(f, Duration.Inf).asInstanceOf[T]
    //Removes dependencies from workflow cache is not needed, this allows them to be unpersisted automatically
    dependencies.foreach(d => p.removeIfUnneeded(d.name))
    p.removeIfUnneeded(this.name)
    t
  }

  protected[dependency] def build(): T

  protected def dep[D <: Task[_]](d: D): D = {
    dependencies += d
    d
  }

  def recursiveVersion: List[String] = {
    name + ":" + version :: dependencies.flatMap(_.recursiveVersion.map("-" + _)).toList
  }
  def recursiveVersionShort: String = {
    val bytes = MD5Hash.digest(recursiveVersion.mkString("\n")).getDigest
    val encodedBytes = Base64.encodeBase64URLSafeString(bytes)
    new String(encodedBytes)
  }
  def exists(): Boolean
  protected def shouldPersist(): Boolean = {
    //If workflow cache is empty then this is probably the exit of the workflow so it
    //essentially has one additional use that's not tracked as a dependency
    p.revDependencies.getOrElse(name, Nil).size > 1 ||
      (p.revDependencies.getOrElse(name, Nil).size == 1 && p.isEmpty)
  }

}






