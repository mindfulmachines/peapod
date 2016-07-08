package generic

import org.apache.spark.{SparkContext, SparkConf}

object Spark {
  val conf = new SparkConf().setAppName( "SparkTest" ).setMaster("local[*]" )
    .set("spark.executor.memory", "1g")

  val sc    = SparkContext.getOrCreate( conf )
}
