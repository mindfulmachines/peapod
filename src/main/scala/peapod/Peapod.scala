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

  lazy val sc = _sc

  val recursiveVersioning = true

  protected val peas: ConcurrentMap[String, Pea[_]] =
    new MapMaker().weakValues().concurrencyLevel(1).makeMap()

  lazy val sqlCtx =  new SQLContext(sc)

  def apply[D: ClassTag](t: Task[D]): Pea[D] = pea(t)

  protected def setLinkages(t: Task[_], p: Pea[_]): Unit = {
    if(! p.exists) {
      t.children.foreach(c => generatePea(c).addParent(p))
      t.children.foreach(c => p.addChild(generatePea(c)))
    }
  }

  protected def generatePea(t: Task[_]): Pea[_] = {
    peas.getOrElseUpdate(
      t.name,
      {
        val p = new Pea(t)
        setLinkages(t,p)
        p
      }
    )
  }

  def pea[D: ClassTag](t: Task[D]): Pea[D] = this.synchronized {
    generatePea(t).asInstanceOf[Pea[D]]
  }

  //TODO: Fix now that Pea's do not load all their children recursivelly, use Task's instead
  def dotFormatDiagram(): String = {
    DotFormatter.format(
      peas.toList.flatMap(
        d => d._2.children.map(
          c => (d._2,c)
        )
      )
    )
  }

  def size() = {
    peas.count(_._2 != null)
  }

}

