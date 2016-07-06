package peapod

import generic.PeapodGenerator
import org.scalatest.FunSuite

class WebTest extends FunSuite {
  class TaskA(implicit val p: Peapod) extends EphemeralTask[Double]  {
    def generate = 1
  }
  class TaskB(implicit val p: Peapod) extends EphemeralTask[Double]  {
    pea(new TaskA())
    def generate = 1
  }
  class TaskC(implicit val p: Peapod) extends EphemeralTask[Double]  {
    pea(new TaskA())
    pea(new TaskB())
    def generate = 1
  }

  test("Web Server") {
    implicit val p = PeapodGenerator.web()
    Thread.sleep(1000)
    p(new TaskC())
    assert(scala.io.Source.fromURL("http://localhost:8080").mkString.trim ==
      "<img src=\"http://graphvizserver-env.elasticbeanstalk.com/?H4sIAAAAAAAAAEvJTC9KLMhQcFeozstPSVWILs5ILEi1TcqviFUqSE0syE_RC09NCkktLlEJSSzOdlICqiipzEm1TckvKUlNibXmwqbMmThljhjKsKvStcPuFlymYlfvjEO9E2711QpFiXnZtsWJualYnQb0J3bhWkI6HbHrdFSqrQUAc6ngZJMBAAA\"></img>"
    )
  }
}
