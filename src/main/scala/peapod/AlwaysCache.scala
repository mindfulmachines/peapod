package peapod

/**
  * Indicates that the Task's output should always be automatically persisted using Spark's persistance mechanism if
  * possible (ie: if it's output is an RDD/DataFrame/DataSet). Otherwise this is only done for Ephemeral Tasks and if
  * there is more than one child currently depending on that Task.
  */
trait AlwaysCache {

}
