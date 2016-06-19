package peapod

import generic.PeapodGenerator
import org.scalatest.FunSuite

class ParamTest extends FunSuite {
  trait ParamA extends Param {
    val a: String
    param(a)
  }
  trait ParamB extends Param {
    val b: String
    param(b)
  }
  class Test(val a: String, val b: String)(implicit val p: Peapod)
    extends EphemeralTask[Double] with ParamA with ParamB {
    def generate = 1
  }

  test("Param") {
    val p = PeapodGenerator.peapod()
    val t1 = new Test("a","b")(p)
    assert(t1.baseName == "peapod.ParamTest$Test_98_97")
    val t2 = new Test("a","a")(p)
    assert(t2.baseName == "peapod.ParamTest$Test_97_97")
  }
}
