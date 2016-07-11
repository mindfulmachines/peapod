package peapod

import java.io.{BufferedReader, InputStreamReader}

import generic.PeapodGeneratorS3
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DoubleWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.FunSuite
import peapod.StorableTask._
import peapod.StorableTaskTest._

class StorableTaskS3Test extends StorableTaskTest {
  override def generatePeapod(): Peapod = PeapodGeneratorS3.peapod()
  override def generatePeapodNonRecursive(): Peapod = PeapodGeneratorS3.peapodNonRecursive()
}
