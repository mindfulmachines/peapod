package peapod

import generic.PeapodGenerator
import org.scalatest.FunSuite
import peapod.TaskTest.{TaskA1, TaskB1, TaskB2, TaskC}

object TaskTest {
  class TaskA1(implicit val p: Peapod) extends EphemeralTask[Double]  {
    override lazy val name = "TaskA"
    override val version = "1"
    override val description = "Return 1 Always"
    def generate = 1
  }
  class TaskA2(implicit val p: Peapod) extends EphemeralTask[Double]  {
    override lazy val name = "TaskA"
    override val version = "2"
    def generate = 1
  }
  class TaskB1(implicit val p: Peapod) extends EphemeralTask[Double]  {
    override lazy val name = "TaskB"
    override val description = "Return 1 Always"
    pea(new TaskA1())
    def generate = 1
  }
  class TaskB2(implicit val p: Peapod) extends EphemeralTask[Double]  {
    override lazy val name = "TaskB"
    pea(new TaskA2())
    def generate = 1
  }
  class TaskC(implicit val p: Peapod) extends EphemeralTask[Double]  {
    def generate = 1
  }
}
class TaskTest  extends FunSuite {
  test("testRecursiveVersion") {
    val p1 = PeapodGenerator.peapod()
    val p2 = PeapodGenerator.peapod()
    val t1 = new TaskB1()(p1)
    val t2 = new TaskB2()(p2)
    assert(t1.recursiveVersion == "TaskB:1" :: "-TaskA:1" :: Nil)
    assert(t2.recursiveVersion== "TaskB:1" :: "-TaskA:2" :: Nil)
    assert(t1.recursiveVersionShort == "_vl0nfo5QL1AWZuHQUaotQ")
    assert(t2.recursiveVersionShort == "eSbl8xEbNGEvh7iKBnDChg")
  }

  test("testRecursiveVersionLatest") {
    val p1 = PeapodGenerator.peapodNonRecursive()
    val p2 = PeapodGenerator.peapodNonRecursive()
    val t1 = new TaskB1()(p1)
    val t2 = new TaskB2()(p2)
    assert(t1.recursiveVersion == "TaskB:1" :: "-TaskA:1" :: Nil)
    assert(t2.recursiveVersion== "TaskB:1" :: "-TaskA:2" :: Nil)
    assert(t1.recursiveVersionShort == "_vl0nfo5QL1AWZuHQUaotQ")
    assert(t2.recursiveVersionShort == "eSbl8xEbNGEvh7iKBnDChg")
  }

  test("testMetaData") {
    val p1 = PeapodGenerator.peapod()
    val p2 = PeapodGenerator.peapod()
    val t1 = new TaskB1()(p1)
    val t2 = new TaskB2()(p2)
    assert(t1.metadata() == "TaskB:1\nReturn 1 Always\n-TaskA:1\n--Return 1 Always")
    assert(t2.metadata() == "TaskB:1\n-TaskA:2")
  }

  test("testEquality") {
    implicit val p1 = PeapodGenerator.peapod()
    val t1 = new TaskB1()
    val t2 = new TaskB2()
    val t3 = new TaskA1()
    assert(t1 == t1)
    assert(t1 == t2)
    assert(t1 != t3)
    assert(t1 != "")
  }

  test("testChildren") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TaskB1()
    assert(t.children == new TaskA1() :: Nil)
    assert(t.childrenArray().toList == new TaskA1() :: Nil)
  }


  test("testName") {
    implicit val p = PeapodGenerator.peapod()
    val t = new TaskC()
    assert(t.name == "peapod.TaskTest$TaskC")
    assert(t.baseName == "peapod.TaskTest$TaskC")
    assert(t.versionName == "peapod.TaskTest$TaskC")
  }

}
