package peapod

import java.util.concurrent.Executors

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.mutable
import scala.collection.immutable.TreeSet
import scala.concurrent.{ExecutionContext, Future}

class Peapod(val fs: String = "s3n://",
              val path: String,
              val raw: String,
              val parallelism: Int = 100,
                val persistentCache: Boolean= false)(implicit val sc: SparkContext) {
  private val cache = new mutable.HashMap[String, Future[_]]
  private val peas = new mutable.HashMap[String, Task[_]]
  private val activePeaLinks = new mutable.HashMap[String, TreeSet[String]]
  val activeReversePeaLinks = new mutable.HashMap[String, TreeSet[String]] with mutable.SynchronizedMap[String, TreeSet[String]]

  private val peaLinks = new mutable.HashMap[String, TreeSet[String]]
  val reversePeaLinks = new mutable.HashMap[String, TreeSet[String]] with mutable.SynchronizedMap[String, TreeSet[String]]
  
  private implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  val sqlCtx =  new SQLContext(sc)

  def isEmpty(): Boolean = {
    cache.isEmpty
  }

  def clear() = this.synchronized {
    cache.clear()
    peas.clear()
    activePeaLinks.clear()
    activeReversePeaLinks.clear()
    peaLinks.clear()
    reversePeaLinks.clear()
  }

  def putActive(d1: Task[_], d2: Task[_]): Unit = this.synchronized {
    if(! d1.exists()) {
      activePeaLinks.update(d1.name, activePeaLinks.getOrElse(d1.name, TreeSet[String]())+ d2.name)
      activeReversePeaLinks.update(d2.name, activeReversePeaLinks.getOrElse(d2.name, TreeSet[String]()) + d1.name )
    }
  }

  def put(d1: Task[_], d2: Task[_]): Unit = this.synchronized {
    peas.getOrElseUpdate(d1.name,d1)
    peas.getOrElseUpdate(d2.name,d2)
    peaLinks.update(d1.name, peaLinks.getOrElse(d1.name, TreeSet[String]())+ d2.name)
    reversePeaLinks.update(d2.name, reversePeaLinks.getOrElse(d2.name, TreeSet[String]()) + d1.name )
  }

  def removeIfUnneeded(name: String): Unit = this.synchronized {
    if(! persistentCache) {
      activeReversePeaLinks.get(name) match {
        case Some(l) =>
          if (l.forall(cache.get(_).forall(_.isCompleted))) {
            cache.remove(name)
          }
        case None =>
          cache.remove(name)
      }
    }
  }
  def build(d: Task[_]): Future[_] = this.synchronized {
    val f= cache.getOrElseUpdate(
      d.name,
      Future {d.build()}
    )
    f
  }

  def clear() = this.synchronized{
    cache.clear()
    peas.clear()
    peaLinks.clear()
    reversePeaLinks.clear()
    activePeaLinks.clear()
    activeReversePeaLinks.clear()
  }


  def dotFormatDiagram(): String = {
    DotFormatter.format(
      peaLinks.toList.flatMap(
        d => d._2.map(
          dd => (peas(d._1),peas(dd))
        )
      )
    )
  }
  def dotFormatActiveDiagram(): String = {
    DotFormatter.format(
      activePeaLinks.toList.flatMap(
        d => d._2.map(
          dd => (peas(d._1),peas(dd))
        )
      )
    )
  }


}

