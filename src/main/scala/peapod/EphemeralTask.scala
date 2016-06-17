package peapod

import org.apache.spark.Logging

import scala.reflect.ClassTag

/**
  * A base Task class which does not store it's output to disk automatically but merely generates and returns it
 */
abstract class EphemeralTask[V: ClassTag]
  extends Task[V] with Logging {

  val storable=  false

  protected def generate: V

  def  build(): V = {
    generate
  }

  /**
    * Returns false always for EphemeralTasks
    */
  def exists(): Boolean = false

  /**
    * Does nothing for EphemeralTasks
    */
  def delete() {}

  /**
    * Generates the output for EphemeralTasks
    */
  def load(): V = {build()}
}
