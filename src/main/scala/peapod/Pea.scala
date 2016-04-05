package peapod

import java.lang.ref.WeakReference
import java.util.concurrent.Executors

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.MD5Hash
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.{HashSet, TreeSet}
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.reflect.ClassTag

/**
  * Created by marcin.mejran on 3/28/16.
  */
class Pea[+D: ClassTag](task: Task[D]) {
  private implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  override val toString = task.name
  override val hashCode = task.name.hashCode
  val version = task.version

  val ephemeral = task.isInstanceOf[EphemeralTask[_]]
  lazy val exists = task.exists()

  var children: Set[Pea[_]] = new HashSet[Pea[_]]()
  var parents: Set[Pea[_]] = new HashSet[Pea[_]]()
  var cache: WeakReference[_] = new WeakReference[D](null.asInstanceOf[D])

  def addParent(pea: Pea[_]) = this.synchronized {
    parents = parents + pea
  }

  def removeParent(pea: Pea[_]) = this.synchronized {
    parents = parents - pea
  }

  def addChild(pea: Pea[_]) = this.synchronized {
    children = children + pea
  }

  def removeChild(pea: Pea[_]) = this.synchronized {
    children = children - pea
  }

  def get(): D = this.synchronized {
    if (cache.get() == null) {
      //For efficiency generate all children get, stored in a list to prevent weak reference loss
      val childGets = if (!exists) {
        children.par.foreach(c => c.get())
      } else {
        Nil
      }
      val f = Future {
        val built = task.build()
        if (parents.size > 1) {
          persist(built)
        } else {
          built
        }
      }
      val d = Await.result(f, Duration.Inf)
      cache = new WeakReference[D](d)
    }
    cache.get().asInstanceOf[D]
  }

  private def persist[V: ClassTag](d: V): V = {
    (
      d match {
        case d: RDD[_] => d.persist(StorageLevel.MEMORY_AND_DISK)
        case d: DataFrame => d.cache()
        case d: Dataset[_] => d.cache()
        case d: V => d
      }
      ).asInstanceOf[V]
  }

  //TODO: Add unpersist logic to PeaPod
  /*private def unpersist[V: ClassTag](d: V): V = {
    (
      d match {
        case d: RDD[_] => d.unpersist()
        case d: DataFrame => d.unpersist()
        case d: Dataset[_] => d.unpersist()
        case d: V => d
      }
      ).asInstanceOf[V]
  }*/

  //TODO: Cache recursive version so dependencies can be removed if not needed
  def recursiveVersion: List[String] = {
    toString + ":" + version :: children.flatMap(_.recursiveVersion.map("-" + _)).toList
  }

  def recursiveVersionShort: String = {
    val bytes = MD5Hash.digest(recursiveVersion.mkString("\n")).getDigest
    val encodedBytes = Base64.encodeBase64URLSafeString(bytes)
    new String(encodedBytes)
  }

  /*
  Define equality and other attributes to be based on the underlying Task classes
  and be unique per Task class
   */
  override def equals(o: Any) = {
    o match {
      case pea: Pea[_] => pea.toString == this.toString
      case _ => false
    }
  }
}

object Pea {
  implicit def getAnyTask[T](pea: Pea[T]): T =
    pea.get()
}
