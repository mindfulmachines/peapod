package peapod

import generic.PeapodGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.{FunSpec, FunSuite}
import peapod.StorableTaskTest._
import StorableTask._
import org.apache.hadoop.io.DoubleWritable

case class Single (value: Double)

object StorableTaskTest {

  class TaskDouble(implicit val p: Peapod) extends StorableTask[Double] {
    def generate = 1
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
  test("testStorage") {
    implicit val p = PeapodGenerator.peapod()
    val task = new TaskDouble()
    val pea = p(task)
    assert(!task.exists())
    pea.get()
    assert(task.exists())
    task.delete()
    assert(!task.exists())
    intercept[java.io.FileNotFoundException] {
      task.load()
    }
    assert(task.build() == 1.0)
    assert(task.load() == 1.0)
  }

  test("testStorageLongTerm") {
    implicit val p = PeapodGenerator.peapod()
    val pea = p(new TaskDouble())
    assert(pea.get() == 1.0)

    val pNew = new Peapod(raw = p.raw, path = p.path)(p.sc)
    assert(new TaskDouble()(pNew).load() == 1.0)

  }

  test("testDouble") {
    implicit val p = PeapodGenerator.peapod()
    val pea = new Pea(new TaskDouble())
    assert(pea() == 1.0)
  }

  test("testRDD") {
    implicit val p = PeapodGenerator.peapod()
    val pea = new Pea(new TaskRDD())
    assert(pea().collect().toList == 1.0 :: 2.0 :: Nil)
  }

  test("testDF") {
    implicit val p = PeapodGenerator.peapod()
    val pea = new Pea(new TaskDF())
    assert(pea().collect().map(_.getAs[Double]("value")).toList == 1.0 :: 2.0 :: Nil)
  }

  test("testDS") {
    implicit val p = PeapodGenerator.peapod()
    val pea = new Pea(new TaskDS())
    assert(pea().collect().map(_.value).toList == 1.0 :: 2.0 :: Nil)
  }

  test("testSerializable") {
    implicit val p = PeapodGenerator.peapod()
    val pea = new Pea(new TaskSerializable())
    assert(pea().value == 1.0)
  }

  test("testWritable") {
    implicit val p = PeapodGenerator.peapod()
    val pea = new Pea(new TaskWritable())
    assert(pea().get() == 1.0)
  }
}
