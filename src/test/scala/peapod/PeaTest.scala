package peapod

import generic.PeapodGenerator
import org.scalatest.FunSuite
import Implicits._

class PeaTest  extends FunSuite {
  class TaskA(implicit val p: Peapod) extends EphemeralTask[Double]  {
    def generate = 1
  }
  class TaskB(implicit val p: Peapod) extends EphemeralTask[Double]  {
    def generate = 1
  }
  class TaskC(implicit val p: Peapod) extends EphemeralTask[Double]  {
    def generate = 1
  }
  class TaskNever(implicit val p: Peapod) extends EphemeralTask[Double]  {
    override val persist = Never
    def generate = 1
  }
  class TaskAlways(implicit val p: Peapod) extends EphemeralTask[Double] {
    override val persist = Always
    def generate = 1
  }

  class TaskDep(implicit val p: Peapod) extends EphemeralTask[Double] {
    override val persist = Always
    val c = pea(new TaskA)
    def generate = c.toDouble
  }

  test("testChildrenParents") {
    implicit val p = PeapodGenerator.peapod()
    val peaA = new Pea(new TaskA())
    val peaB = new Pea(new TaskB())
    val peaC = new Pea(new TaskC())
    assert(peaB.children == Set())
    assert(peaB.parents == Set())
    peaB.addChild(peaA)
    assert(peaB.children == Set(peaA))
    peaB.removeChild(peaA)
    assert(peaB.children == Set())
    peaB.addParent(peaC)
    assert(peaB.parents == Set(peaC))
    peaB.removeParent(peaC)
    assert(peaB.parents == Set())
  }

  test("testRemoval") {
    implicit val p = PeapodGenerator.peapod()
    val peaA = new Pea(new TaskA())
    val peaB = new Pea(new TaskB())
    assert(peaA.children == Set())
    assert(peaA.parents == Set())
    assert(peaB.children == Set())
    assert(peaB.parents == Set())
    peaB.addChild(peaA)
    peaA.addParent(peaB)
    assert(peaB.children == Set(peaA))
    assert(peaA.parents == Set(peaB))
    peaA.get()
    assert(peaB.children == Set())
    assert(peaB.parents == Set())
    assert(peaA.children == Set())
    assert(peaA.parents == Set())
  }

  test("testName") {
    implicit val p = PeapodGenerator.peapod()
    val task = new TaskA()
    val pea = new Pea(task)
    assert(pea.toString == "peapod.PeaTest$TaskA")
  }

  test("testImplicits") {
    implicit val p = PeapodGenerator.peapod()
    val task = new TaskDep()
    val pea = new Pea(task)
    assert(pea.toDouble == 1.0)
  }

  test("testGet") {
    implicit val p = PeapodGenerator.peapod()
    val task = new TaskA()
    val pea = new Pea(task)
    assert(pea.get() == 1.0)
    assert(pea() == 1.0)
  }

  test("testCache") {
    implicit val p = PeapodGenerator.peapod()
    val pea = new Pea(new TaskA())
    assert(pea.cache.isEmpty)
    pea.get()
    assert(pea.cache.contains(1.0))
    assert(pea.get() == 1.0)
  }

  test("testCacheNever") {
    implicit val p = PeapodGenerator.peapod()
    val pea = new Pea(new TaskNever())
    assert(pea.get() == 1.0)
    assert(pea.get() == 1.0)
  }

  test("testCacheAlways") {
    implicit val p = PeapodGenerator.peapod()
    val pea = new Pea(new TaskAlways())
    assert(pea.get() == 1.0)
    assert(pea.get() == 1.0)
  }

  test("testEquals") {
    implicit val p = PeapodGenerator.peapod()
    val peaA = new Pea(new TaskA())
    val peaB = new Pea(new TaskB())
    assert(peaA == peaA)
    assert(peaA != peaB)
    //noinspection ComparingUnrelatedTypes
    assert(peaA != "")
  }
}
