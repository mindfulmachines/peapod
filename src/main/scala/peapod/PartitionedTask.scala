package peapod

import org.joda.time.LocalDate

import scala.reflect.ClassTag

/**
  * Created by marcin.mejran on 4/1/16.
  */
trait PartitionedTask[D] extends Task[D] {
  val partition: LocalDate
  override lazy val name: String = baseName+ "/" + partition.toString()
  override lazy val versionName = baseName
  override def dir(p: Peapod) = p.path + "/" + baseName + "/" + recursiveVersionShort(p) + "/" +
    partition.toString("yyyy/MM/dd") + "/"
}
