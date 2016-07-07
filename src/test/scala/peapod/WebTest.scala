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
    val t = p(new TaskC())
    Thread.sleep(1000)
    assert(scala.io.Source.fromURL("http://localhost:8080/graph").mkString.trim ==
      """{"nodes":[{"name":"peapod.WebTest$TaskA","ephemeral":true,"exists":false},{"name":"peapod.WebTest$TaskB","ephemeral":true,"exists":false},{"name":"peapod.WebTest$TaskC","ephemeral":true,"exists":false}],"edges":[{"nodeA":"peapod.WebTest$TaskA","nodeB":"peapod.WebTest$TaskB"},{"nodeA":"peapod.WebTest$TaskA","nodeB":"peapod.WebTest$TaskC"},{"nodeA":"peapod.WebTest$TaskB","nodeB":"peapod.WebTest$TaskC"}]}"""
    )
    assert(scala.io.Source.fromURL("http://localhost:8080/graph?active").mkString.trim ==
      """{"nodes":[{"name":"peapod.WebTest$TaskA","ephemeral":true,"exists":false},{"name":"peapod.WebTest$TaskB","ephemeral":true,"exists":false},{"name":"peapod.WebTest$TaskC","ephemeral":true,"exists":false}],"edges":[{"nodeA":"peapod.WebTest$TaskA","nodeB":"peapod.WebTest$TaskB"},{"nodeA":"peapod.WebTest$TaskB","nodeB":"peapod.WebTest$TaskC"},{"nodeA":"peapod.WebTest$TaskA","nodeB":"peapod.WebTest$TaskC"}]}"""
    )
    p(new TaskB()).get()
    System.gc()
    Thread.sleep(1000)
    assert(scala.io.Source.fromURL("http://localhost:8080/graph?active").mkString.trim ==
      """{"nodes":[{"name":"peapod.WebTest$TaskB","ephemeral":true,"exists":false},{"name":"peapod.WebTest$TaskA","ephemeral":true,"exists":false},{"name":"peapod.WebTest$TaskC","ephemeral":true,"exists":false}],"edges":[{"nodeA":"peapod.WebTest$TaskB","nodeB":"peapod.WebTest$TaskC"},{"nodeA":"peapod.WebTest$TaskA","nodeB":"peapod.WebTest$TaskC"}]}"""
    )

  }
}
