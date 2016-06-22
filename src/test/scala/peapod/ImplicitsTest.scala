package peapod

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class ImplicitsTest extends FunSuite{

  import Implicits._

  test("testConfig") {
    val conf = ConfigFactory.empty()
      .set("test", 42)
      .set("test2", "test")
    assert(conf.getInt("test")== 42)
    assert(conf.getString("test2")== "test")
  }
}
