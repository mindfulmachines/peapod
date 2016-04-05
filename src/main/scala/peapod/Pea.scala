package peapod

import java.util.concurrent.Executors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.TreeSet
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.reflect.ClassTag

/**
  * Created by marcin.mejran on 3/28/16.
  */
class Pea[D: ClassTag, T <: Task[D]: ClassTag](task: T) {
  override val toString= task.name
  override val hashCode = task.hashCode
  val ephemeral = task.isInstanceOf[EphemeralTask[_]]
  val exists = task.exists()

  private implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  var children: Set[Pea[_,_]] = new TreeSet[Pea[_, _]]()
  var parents: Set[Pea[_,_]] = new TreeSet[Pea[_, _]]()
  var cache: Future[D] = Future(task.get())
  var root = false

  def addParent(pea: Pea[_,_]) = this.synchronized {
    parents = parents + pea
  }
  def removeParent(pea: Pea[_,_]) = this.synchronized {
    parents = parents - pea
  }

  def setAsRoot() = this.synchronized {
    root = true
  }
  def addChild(pea: Pea[_,_]) = this.synchronized {
    children = children + pea
  }
  def removeChild(pea: Pea[_,_]) = this.synchronized {
    children = children - pea
  }
  def get(): D = this.synchronized {
    val data = Await.result(cache, Duration.Inf)
    (
      data match {
        case d: RDD[_] => d.persist(StorageLevel.MEMORY_AND_DISK)
        case d: DataFrame => d.cache()
        case d: Dataset => d.cache()
        case _ => _
      }
      ).asInstanceOf[D]
  }

  /*
  Define equality and other attributes to be based on the underlying Task classes
  and be unique per Task class
   */
  override def equals(o: Any) = {
    o match {
      case pea: Pea[_,_] => pea.toString == this.toString
      case _ => false
    }
  }
}
