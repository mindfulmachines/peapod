package peapod

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.MD5Hash

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

abstract class Task [+T: ClassTag](implicit p: Peapod) extends BaseTask {
  protected val peas= scala.collection.mutable.ArrayBuffer.empty[Task[_]]

  def get(): T = {
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
  }

  protected[peapod] def build(): T

  protected def pea[D <: Task[_]](d: D): D = {
    peas += d
    p.put(this,d)
    d
  }
  def recursiveVersion: List[String] = {
    versionName + ":" + version :: peas.flatMap(_.recursiveVersion.map("-" + _)).toList
  }

  protected def shouldPersist(): Boolean = {
    //If workflow cache is empty then this is probably the exit of the workflow so it
    //essentially has one additional use that's not tracked as a dependency
    p.activeReversePeaLinks.getOrElse(name, Nil).size > 1 ||
      (p.activeReversePeaLinks.getOrElse(name, Nil).size == 1 && p.isEmpty)
  }

}



object Task {
  implicit def getAnyTask[T](task: Task[T]): T =
    task.get()
}


