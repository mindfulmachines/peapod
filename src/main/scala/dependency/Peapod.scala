package dependency

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
  private val dependencies = new mutable.HashMap[String, TreeSet[String]]
  val revDependencies = new mutable.HashMap[String, TreeSet[String]] with mutable.SynchronizedMap[String, TreeSet[String]]
  private implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  val sqlCtx =  new SQLContext(sc)

  def isEmpty(): Boolean = {
    cache.isEmpty
  }

  def put(d1: Task[_], d2: Task[_]): Unit = this.synchronized {
    if(! d1.exists()) {
      dependencies.update(d1.name, dependencies.getOrElse(d1.name, TreeSet[String]())+ d2.name)
      revDependencies.update(d2.name, revDependencies.getOrElse(d2.name, TreeSet[String]()) + d1.name )
    }
  }
  def removeIfUnneeded(name: String): Unit = this.synchronized {
    if(! persistentCache) {
      revDependencies.get(name) match {
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
}

