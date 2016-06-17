package peapod

import java.util.concurrent.ConcurrentMap

import com.google.common.collect.MapMaker
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.reflect.ClassTag
import collection.JavaConversions._

class Peapod( val path: String,
              val raw: String,
              val conf: Config = ConfigFactory.empty())(_sc : => SparkContext) {

  /**
    * Spark Context
    */
  lazy val sc = _sc

  /**
    * SQL Spark Context
    */
  lazy val sqlCtx =  new SQLContext(sc)

  /**
    * Is recursive versioning enabled, used by classes which extend Peapod
    */
  val recursiveVersioning = true

  protected val peas: ConcurrentMap[String, Pea[_]] =
    new MapMaker().weakValues().concurrencyLevel(1).makeMap()

  protected val tasks: ConcurrentMap[String, Task[_]] =
    new MapMaker().weakValues().concurrencyLevel(1).makeMap()

  /**
    * Returns a Pea for a Task and caches the Pea
    */
  def apply[D: ClassTag](t: Task[D]): Pea[D] = pea(t)

  protected def setLinkages(t: Task[_], p: Pea[_]): Unit = {
    //TODO: This is hacky, should be moved into the Pea and centralized in terms of the logic location
    if(! p.task.exists) {
      t.children.foreach(c => generatePea(c).addParent(p))
      t.children.foreach(c => p.addChild(generatePea(c)))
    }
  }

  protected def generatePea(t: Task[_]): Pea[_] = {
    tasks.update(t.name,t)
    peas.getOrElseUpdate(
      t.name,
      {
        val p = new Pea(t)
        setLinkages(t,p)
        p
      }
    )
  }

  /**
    * Returns a Pea for a Task and caches the Pea
    */
  def pea[D: ClassTag](t: Task[D]): Pea[D] = this.synchronized {
    generatePea(t).asInstanceOf[Pea[D]]
  }

  /**
    * Returns the Peapod's Task's in a DOT format graph
    */
  def dotFormatDiagram(): String = {
    DotFormatter.format(
      tasks.toList.flatMap(
        d => d._2.children.map(
          c => (d._2,c)
        )
      )
    )
  }

  /**
    * Returns the number of Tasks that have been cached by this Peapod instance
    */
  def size() = {
    tasks.count(_._2 != null)
  }

}

