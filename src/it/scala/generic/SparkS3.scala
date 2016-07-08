package generic

import com.amazonaws.auth.{AWSCredentialsProviderChain, DefaultAWSCredentialsProviderChain}
import org.apache.spark.{SparkConf, SparkContext}

object SparkS3 {
  val conf = new SparkConf().setAppName( "SparkTest" ).setMaster("local[*]" )
    .set("spark.executor.memory", "1g")

  val sc    = SparkContext.getOrCreate( conf )

  val credentials = new DefaultAWSCredentialsProviderChain().getCredentials

  sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId",credentials.getAWSAccessKeyId)
  sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",credentials.getAWSSecretKey)
}
