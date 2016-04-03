package peapod

import org.joda.time.LocalDate

import scala.reflect.ClassTag

/**
  * Created by marcin.mejran on 4/1/16.
  */
trait PartitionedTask {
  self: BaseTask =>
  val partition: LocalDate
  override lazy val name: String = baseName+ "/" + partition.toString()
  override lazy val versionName = baseName
  override lazy val dir = p.path + "/" + baseName + "/" + recursiveVersionShort + "/" +
    partition.toString("yyyy/MM/dd") + "/"
}
