package peapod

import java.util.concurrent.Executors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.HashSet
import scala.collection.parallel.ExecutionContextTaskSupport
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

/**
  * The in memory representation of a Task, provides caching of output. Should not be instantiated directly but only
  * using Peapod() or Peapod.pea().
  */
class Pea[+D: ClassTag](val task: Task[D]) extends Logging {
  /**
    * Name of the Pea instances, used to uniquely identify a Pea, all Pea's with the same name should be identical
    */
  override val toString = task.name

  /**
    * Hashcode is the name's hascode
    */
  override val hashCode = task.name.hashCode

  /**
    * Peas which this Pea is dependent on
    */
  var parents: Set[Pea[_]] = new HashSet[Pea[_]]()

  /**
    * Peas which depend on this Pea
    */
  var children: Set[Pea[_]] = new HashSet[Pea[_]]()

  /**
    * Store the output of the Task this Pea reperesents
    */
  var cache: Option[_] = None

  private[peapod] def addChild(pea: Pea[_]) = children.synchronized {
    children = children + pea
  }


  private[peapod] def removeChild(pea: Pea[_]) = children.synchronized {
    children = children - pea
    if(children.isEmpty) {
      cache match {
        case Some(c) => //unpersist(c.asInstanceOf[D])
        case None =>
      }
    }
  }


  private[peapod] def addParent(pea: Pea[_]) = parents.synchronized {
    parents = parents + pea
  }

  private[peapod] def removeParent(pea: Pea[_]) = parents.synchronized {
    parents = parents - pea
  }

  def apply(): D = get()

  /**
    * Generates the output of the Task this Pea represents, either by loading it from storage or generating it on the
    * fly
    */
  def build(): D = {
    if(! task.exists()) {
      logInfo("Loading" + this + " Deleting")
      task.delete()
      logInfo("Loading" + this + " Generating")
      task.build()
    } else {
      logInfo("Loading" + this + " Reading")
      task.load()
    }
  }

  /**
    * Generates the cache for this Pea, updating stale parents and children in the process and managing persistance
    */

  protected def buildCache(): Unit = {
    cache = cache match {
      case None =>
        if (!task.exists) {
          val par = parents.par
          par.tasksupport = Pea.tasksupport
          par.foreach(c => c.get())
        }
        val d = {
          val built = build()
          task.persist match {
            case Auto =>
              if (children.size > 1) {
                persist(built)
              } else {
                built
              }
            case Always => persist(built)
            case Never => built
          }
        }
        Some(d)
      case Some(c) =>
        task.persist match {
          case Auto =>
            if (children.size > 1) {
              Some(persist(c))
            } else {
              Some(c)
            }
          case Always => Some(persist(c))
          case Never => Some(c)
        }
    }


    parents.foreach(p => p.removeChild(this))
    parents.foreach(p => p.unpersist())
    parents = parents.empty

  }

  /**
    * Returns the output of the Task this PEa represents and stores the result in the Pea's cache
    */
  def get(): D = this.synchronized {
    buildCache()
    val d = cache match {
      case None =>
        throw new Exception("Cache is empty after being set")
      case Some(c) =>
        c.asInstanceOf[D]
    }
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

  /**
    * Unpersists the object currently stored in this Pea's cache and updates the cache with the unpersisted version
    */
  private[Pea] def unpersist(): Unit = this.synchronized {
    cache = cache match {
      case None => None
      case Some(c) =>
        task.persist match {
          case Auto =>
            if (children.isEmpty) {
              Some(unpersist(c))
            } else {
              Some(c)
            }
          case Always => Some(c)
          case Never => Some(unpersist(c))
        }
    }
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
  private val tasksupport =
    new ExecutionContextTaskSupport(
      ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
    )
}
