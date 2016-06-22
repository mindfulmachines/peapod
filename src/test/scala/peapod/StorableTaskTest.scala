package peapod

import java.io.{BufferedReader, InputStreamReader}

import generic.PeapodGenerator
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DoubleWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.FunSuite
import peapod.StorableTask._
import peapod.StorableTaskTest._

case class Single (value: Double)

object StorableTaskTest {

  class TaskA1(implicit val p: Peapod) extends StorableTask[Double]  {
    override lazy val baseName = "TaskA"
    override val version = "1"
    override val description = "Return 1 Always"
    def generate = 1
  }

  class TaskA2(implicit val p: Peapod) extends StorableTask[Double]  {
    override lazy val baseName = "TaskA"
    override val version = "2"
    def generate = 1
  }

  class TaskB1(implicit val p: Peapod) extends StorableTask[Double]  {
    override lazy val baseName = "TaskB"
    override val description = "Return 1 Always"
    pea(new TaskA1())
    def generate = 1
  }

  class TaskB2(implicit val p: Peapod) extends StorableTask[Double]  {
    override lazy val baseName = "TaskB"
    pea(new TaskA2())
    def generate = 1
  }

  class TaskDouble(implicit val p: Peapod) extends StorableTask[Double] {
    def generate = 1.0
  }

  class TaskInt(implicit val p: Peapod) extends StorableTask[Int] {
    def generate = 1
  }

  class TaskLong(implicit val p: Peapod) extends StorableTask[Long] {
    def generate = 1l
  }

  class TaskBoolean(implicit val p: Peapod) extends StorableTask[Boolean] {
    def generate = true
  }

  class TaskRDD(implicit val p: Peapod) extends StorableTask[RDD[Double]] {
    def generate = p.sc.makeRDD(1.0 :: 2.0 :: Nil)
  }

  class TaskDF(implicit val p: Peapod) extends StorableTask[DataFrame] {
    def generate = {
      import p.sqlCtx.implicits._
      p.sc.makeRDD( Tuple1(1.0) :: Tuple1(2.0) :: Nil).toDF("value")
    }
  }

  class TaskDS(implicit val p: Peapod) extends StorableTask[Dataset[Single]] {
    def generate = {
      import p.sqlCtx.implicits._
      p.sc.makeRDD( Single(1.0) :: Single(2.0) :: Nil).toDS()
    }
  }

  class TaskSerializable(implicit val p: Peapod) extends StorableTask[Single] {
    def generate = {
      Single(1.0)
    }
  }

  class TaskWritable(implicit val p: Peapod) extends StorableTask[DoubleWritable] {
    def generate = {
      new DoubleWritable(1.0)
    }
  }
}
class StorableTaskTest extends FunSuite {
  test("Delete Old Version") {
    val p1 = PeapodGenerator.peapod()
    val t1 = new TaskB1()(p1)
    val t2 = new TaskB2()(p1)
    t1.build()
    t2.build()
    assert(t1.exists())
    assert(t2.exists())
    t2.deleteOtherVersions()
    assert(!t1.exists())
    assert(t2.exists())
  }


  test("Recursive Version") {
    val p1 = PeapodGenerator.peapod()
    val p2 = PeapodGenerator.peapod()
    val t1 = new TaskB1()(p1)
    val t2 = new TaskB2()(p2)
    assert(t1.recursiveVersion == "TaskB:1" :: "-TaskA:1" :: Nil)
    assert(t2.recursiveVersion== "TaskB:1" :: "-TaskA:2" :: Nil)
    assert(t1.recursiveVersionShort == "_vl0nfo5QL1AWZuHQUaotQ")
    assert(t2.recursiveVersionShort == "eSbl8xEbNGEvh7iKBnDChg")
    assert(t1.dir.endsWith("TaskB/_vl0nfo5QL1AWZuHQUaotQ"))
    assert(t2.dir.endsWith("TaskB/eSbl8xEbNGEvh7iKBnDChg"))
  }

  test("Recursive Version Latest") {
    val p1 = PeapodGenerator.peapodNonRecursive()
    val p2 = PeapodGenerator.peapodNonRecursive()
    val t1 = new TaskB1()(p1)
    val t2 = new TaskB2()(p2)
    assert(t1.recursiveVersion == "TaskB:1" :: "-TaskA:1" :: Nil)
    assert(t2.recursiveVersion== "TaskB:1" :: "-TaskA:2" :: Nil)
    assert(t1.recursiveVersionShort == "_vl0nfo5QL1AWZuHQUaotQ")
    assert(t2.recursiveVersionShort == "eSbl8xEbNGEvh7iKBnDChg")
    assert(t1.dir.endsWith("TaskB/latest"))
    assert(t2.dir.endsWith("TaskB/latest"))
  }


  test("SUCCESS file") {
    implicit val p = PeapodGenerator.peapod()
    val task = new TaskDouble()
    task.build()
    val path = new Path(task.dir + "/_SUCCESS")
    val fs = path.getFileSystem(p.sc.hadoopConfiguration)
    assert(fs.exists(path))

  }

  test("Metadata file") {
    implicit val p = PeapodGenerator.peapod()
    val task = new TaskDouble()
    task.build()
    val path = new Path(task.dir + "/_peapod_metadata")
    val fs = path.getFileSystem(p.sc.hadoopConfiguration)
    assert(fs.exists(path))
    val br=new BufferedReader(new InputStreamReader(fs.open(path)))
    val meta = Stream.continually(br.readLine()).takeWhile(_ != null).mkString("\n")
    assert(meta == task.metadata())
    fs.close()
  }

  test("Storage") {
    implicit val p = PeapodGenerator.peapod()
    val task = new TaskDouble()
    assert(!task.exists())
    task.build()
    assert(task.exists())
    task.delete()
    assert(!task.exists())
    intercept[java.io.FileNotFoundException] {
      task.load()
    }
    assert(task.build() == 1.0)
    assert(task.load() == 1.0)
  }

  test("StorageLongTerm") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TaskDouble()
    assert(t.build() == 1.0)

    val pNew = new Peapod(raw = p.raw, path = p.path)(p.sc)
    assert(new TaskDouble()(pNew).load() == 1.0)

  }

  test("Double") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TaskDouble()
    assert(t.build() == 1.0)
  }

  test("Int") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TaskInt()
    assert(t.build() == 1)
  }

  test("Long") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TaskLong()
    assert(t.build() == 1l)
  }

  test("Boolean") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TaskBoolean()
    assert(t.build())
  }

  test("RDD") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TaskRDD()
    assert(t.build().collect().toList == 1.0 :: 2.0 :: Nil)
  }

  test("DF") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TaskDF()
    assert(t.build().collect().map(_.getAs[Double]("value")).toList == 1.0 :: 2.0 :: Nil)
  }

  test("DS") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TaskDS()
    assert(t.build().collect().map(_.value).toList == 1.0 :: 2.0 :: Nil)
  }

  test("Serializable") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TaskSerializable()
    assert(t.build().value == 1.0)
  }

  test("Writable") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TaskWritable()
    assert(t.build().get() == 1.0)
  }
}
