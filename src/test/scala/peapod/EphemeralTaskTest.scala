package peapod

import generic.PeapodGenerator
import org.scalatest.FunSuite

class EphemeralTaskTest extends FunSuite {
  class TaskDouble(implicit val p: Peapod) extends EphemeralTask[Double] {
    def generate = 1
  }

  test("testStorage") {
    implicit val p = PeapodGenerator.peapod()
    val task = new TaskDouble()
    val pea = p(task)
    assert(!task.exists())
    pea.get()
    assert(!task.exists())
    task.delete()
    assert(!task.exists())
    assert(task.load() == 1.0)
    assert(task.build() == 1.0)
    assert(task.load() == 1.0)
  }

}
