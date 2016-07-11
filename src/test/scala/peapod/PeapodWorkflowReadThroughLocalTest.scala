package peapod

import com.typesafe.config.ConfigFactory
import generic.PeapodGenerator
import Implicits._

class PeapodWorkflowReadThroughLocalTest extends PeapodWorkflowTest {
  def generatePeapod() = {PeapodGenerator.peapod( ConfigFactory.load().set("peapod.storable.readthrough",true))}

}
