package peapod

/**
  * Indicates that the Task's output should never be automatically persisted using Spark's persistance mechanism if
  * possible (ie: if it's output is an RDD/DataFrame/DataSet).
  */
trait NeverCache {

}
