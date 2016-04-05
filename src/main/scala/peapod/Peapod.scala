package peapod

import java.util.concurrent.Executors

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.mutable
import scala.collection.immutable.TreeSet
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

class Peapod(val fs: String = "s3n://",
              val path: String,
              val raw: String,
              val parallelism: Int = 100,
                val persistentCache: Boolean= false)(implicit val sc: SparkContext) {
  private val peas = new mutable.WeakHashMap[String, Pea[_]]


  private implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  val sqlCtx =  new SQLContext(sc)

  def pea[D: ClassTag](d: Task[D]): Pea[D] = this.synchronized {
    val f= peas.getOrElseUpdate(
      d.name,
      new Pea(d)
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


}

