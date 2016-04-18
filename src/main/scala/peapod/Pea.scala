package peapod

import java.lang.ref.WeakReference
import java.util.concurrent.Executors

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.MD5Hash
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.{HashSet, TreeSet}
import scala.collection.parallel.{ExecutionContextTaskSupport, ForkJoinTaskSupport}
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.reflect.ClassTag

/**
  * Created by marcin.mejran on 3/28/16.
  */
class Pea[+D: ClassTag](task: Task[D]) {
  override val toString = task.name
  lazy val versionName = task.versionName
  override val hashCode = task.name.hashCode
  lazy val version = task.version

  lazy val ephemeral = task.isInstanceOf[EphemeralTask[_]]
  lazy val exists = task.exists()

  var children: Set[Pea[_]] = new HashSet[Pea[_]]()
  var parents: Set[Pea[_]] = new HashSet[Pea[_]]()
  var cache: Option[_] = None

  def addParent(pea: Pea[_]) = this.synchronized {
    parents = parents + pea
  }

  def removeParent(pea: Pea[_]) = this.synchronized {
    parents = parents - pea
    if(parents.isEmpty) {
      cache match {
        case Some(c) => unpersist(c.asInstanceOf[D])
        case None =>
      }
    }
  }

  def addChild(pea: Pea[_]) = this.synchronized {
    children = children + pea
  }

  def removeChild(pea: Pea[_]) = this.synchronized {
    children = children - pea
  }

  def apply(): D = get()

  def get(): D = this.synchronized {
    val d = cache match {
      case None =>
        if (!exists) {
          val par = children.par
          par.tasksupport = Pea.tasksupport
          par.foreach(c => c.get())
        }
        val d = {
          val built = task.build()
          if (parents.size > 1) {
            persist(built)
          } else {
            built
          }
        }
        cache = Some(d)
        d.asInstanceOf[D]
      case Some(c) => c.asInstanceOf[D]
    }
    children.foreach(c => c.removeParent(this))
    children = children.empty
    d
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

  private def unpersist[V: ClassTag](d: V): V = {
    (
      d match {
        case d: RDD[_] => d.unpersist()
        case d: DataFrame => d.unpersist()
        case d: Dataset[_] => d.unpersist()
        case d: V => d
      }
      ).asInstanceOf[V]
  }

  lazy val recursiveVersion: List[String] = {
    //Sorting first so that changed in ordering of peas doesn't cause new version
    versionName + ":" + version :: children.toList.sortBy(_.versionName).flatMap(_.recursiveVersion.map("-" + _)).toList
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
  implicit def getAnyTask[T: ClassTag](task: Task[T]): Pea[T] =
    task.p(task)

  implicit def getAnyTask[T: ClassTag](pea: Pea[T]): T =
    pea.get()



  private val tasksupport =
    new ExecutionContextTaskSupport(
      ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
    )
}
