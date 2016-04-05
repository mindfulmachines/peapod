package peapod

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.MD5Hash

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

abstract class Task [+T: ClassTag](implicit p: Peapod) {
  lazy val name: String = this.getClass.getName
  val version: String = "1"
  protected lazy val pea: Pea[T] = p.pea[T](this)

  protected lazy val dir = p.fs + p.path + "/" + name + "/" + pea.recursiveVersionShort
/*
  private[peapod] def get(): T = {
    //Adds all dependencies to workflow
    peas.foreach(d => p.putActive(this, d))
    //Builds all dependencies
    if(! exists()) {
      peas.foreach(d => p.build(d))
    }
    val f= p.build(this)
    val t= Await.result(f, Duration.Inf).asInstanceOf[T]
    //Removes dependencies from workflow cache is not needed, this allows them to be unpersisted automatically
    peas.foreach(d => p.removeIfUnneeded(d.name))
    p.removeIfUnneeded(this.name)
    t
  }*/

  protected[peapod] def build(): T

  protected def pea[D: ClassTag](t: Task[D]): Pea[D] = {
    val child = p.pea(t)
    pea.addChild(child)
    child.addParent(pea)
    child
  }

  def exists(): Boolean

}
