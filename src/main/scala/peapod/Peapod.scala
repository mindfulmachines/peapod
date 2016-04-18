package peapod

import java.util.concurrent.{ConcurrentMap, Executors}

import com.google.common.collect.MapMaker
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.concurrent.{ExecutionContext}
import scala.reflect.ClassTag
import collection.JavaConversions._

class Peapod(private[peapod] val path: String,
              val raw: String,
             private val persistentCache: Boolean= false)(implicit val sc: SparkContext) {
  protected val peas: ConcurrentMap[String, Pea[_]] = new MapMaker().weakValues().makeMap()

  protected implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  val sqlCtx =  new SQLContext(sc)

  def apply[D: ClassTag](t: Task[D]): Pea[D] = pea(t)

  def pea[D: ClassTag](t: Task[D]): Pea[D] = {
    val f= peas.getOrElseUpdate(
      t.name,
      {
        val p = new Pea(t)
        t.children.foreach(c => pea(c).addParent(p))
        t.children.foreach(c => p.addChild(pea(c)))
        p
      }
    ).asInstanceOf[Pea[D]]
    f
  }


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

